// Structures used in Group Store
//  File System
//  /acct/(uuid)/fs  "(uuid)"    { "id": "uuid", "name": "name", "status": "active",
//                                "createdate": <timestamp>, "deletedate": <timestamp>
//                               }
//
// IP Address
// /acct/(uuid)/fs/(uuid)/addr "(uuid)"   { "id": uuid, "addr": "111.111.111.111", "status": "active",
//                                         "createdate": <timestamp>, "deletedate": <timestamp>
//                                       }

package formic

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
	"time"

	pb "github.com/getcfs/megacfs/formic/formicproto"
	"github.com/gholt/brimtime"
	"github.com/gholt/store"
	"github.com/spaolacci/murmur3"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
)

var errf = grpc.Errorf

// AcctPayLoad ...
type AcctPayLoad struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Token      string `json:"token"`
	Status     string `json:"status"`
	CreateDate int64  `json:"createdate"`
	DeleteDate int64  `json:"deletedate"`
}

// TokenRef ...
type TokenRef struct {
	TokenID string `json:"token"`
	AcctID  string `json:"acctid"`
}

// FileSysRef ...
type FileSysRef struct {
	FSID   string `json:"fsid"`
	AcctID string `json:"acctid"`
}

// FileSysAttr ...
type FileSysAttr struct {
	Attr  string `json:"attr"`
	Value string `json:"value"`
	FSID  string `json:"fsid"`
}

// AddrRef ...
type AddrRef struct {
	Addr string `json:"addr"`
	FSID string `json:"fsid"`
}

// FileSysMeta ...
type FileSysMeta struct {
	ID     string   `json:"id"`
	AcctID string   `json:"acctid"`
	Name   string   `json:"name"`
	Status string   `json:"status"`
	Addr   []string `json:"addrs"`
}

func clear(v interface{}) {
	p := reflect.ValueOf(v).Elem()
	p.Set(reflect.Zero(p.Type()))
}

// FileSystemAPIServer is used to implement oohhc
type FileSystemAPIServer struct {
	gstore       store.GroupStore
	vstore       store.ValueStore
	log          *zap.Logger
	authUrl      string
	authUser     string
	authPassword string
	skipAuth     bool
}

// FSAttrList ...
var FSAttrList = []string{"name"}

// NewFileSystemAPIServer ...
func NewFileSystemAPIServer(cfg *Config, grpstore store.GroupStore, valstore store.ValueStore, logger *zap.Logger) *FileSystemAPIServer {
	s := &FileSystemAPIServer{
		gstore:       grpstore,
		vstore:       valstore,
		log:          logger,
		authUrl:      cfg.AuthUrl,
		authUser:     cfg.AuthUser,
		authPassword: cfg.AuthPassword,
		skipAuth:     cfg.SkipAuth,
	}

	return s
}

// GrantAddrFS ...
func (s *FileSystemAPIServer) GrantAddrFS(ctx context.Context, r *pb.GrantAddrFSRequest) (*pb.GrantAddrFSResponse, error) {
	var err error
	var acctID string
	var fsRef FileSysRef
	var value []byte
	var addrData AddrRef
	var addrByte []byte
	srcAddr := ""
	srcAddrIP := ""

	// Get incomming ip
	pr, ok := peer.FromContext(ctx)
	if ok {
		srcAddr = pr.Addr.String()
	}
	// validate token
	acctID, err = s.validateToken(r.Token)
	if err != nil {
		s.log.Info("GRANT FAILED", zap.String("src", srcAddr), zap.String("error", "PermissionDenied"))
		return nil, errf(codes.PermissionDenied, "%v", "Invalid Token")
	}
	log := s.log.With(zap.String("src", srcAddr), zap.String("acct", acctID), zap.String("fsid", r.FSid))

	// Read FileSysRef entry to determine if it exists and Account matches
	pKey := fmt.Sprintf("/fs")
	pKeyA, pKeyB := murmur3.Sum128([]byte(pKey))
	cKeyA, cKeyB := murmur3.Sum128([]byte(r.FSid))
	_, value, err = s.gstore.Read(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, nil)
	if store.IsNotFound(err) {
		log.Info("GRANT FAILED", zap.String("error", "IDNotFound"))
		return nil, errf(codes.NotFound, "%v", "File System ID Not Found")
	}
	if err != nil {
		log.Error("GRANT FAILED", zap.Error(err))
		return nil, errf(codes.Internal, "%v", err)
	}
	err = json.Unmarshal(value, &fsRef)
	if err != nil {
		log.Error("GRANT FAILED", zap.Error(err))
		return nil, errf(codes.Internal, "%v", err)
	}
	if fsRef.AcctID != acctID {
		log.Info("GRANT FAILED", zap.String("error", "AccountMismatch"), zap.String("acc2", fsRef.AcctID))
		return nil, errf(codes.FailedPrecondition, "%v", "Account Mismatch")
	}

	// GRANT an file system entry for the addr
	// 		write /fs/FSID/addr			addr						AddrRef
	if r.Addr == "" {
		srcAddrIP = strings.Split(srcAddr, ":")[0]
	} else {
		srcAddrIP = r.Addr
	}
	pKey = fmt.Sprintf("/fs/%s/addr", r.FSid)
	pKeyA, pKeyB = murmur3.Sum128([]byte(pKey))
	cKeyA, cKeyB = murmur3.Sum128([]byte(srcAddrIP))
	timestampMicro := brimtime.TimeToUnixMicro(time.Now())
	addrData.Addr = srcAddrIP
	addrData.FSID = r.FSid
	addrByte, err = json.Marshal(addrData)
	if err != nil {
		log.Error("GRANT FAILED", zap.Error(err))
		return nil, errf(codes.Internal, "%v", err)
	}
	_, err = s.gstore.Write(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, timestampMicro, addrByte)
	if err != nil {
		log.Error("GRANT FAILED", zap.Error(err))
		return nil, errf(codes.Internal, "%v", err)
	}

	// return Addr was Granted
	// Log Operation
	log.Info("GRANT", zap.String("addr", srcAddrIP))
	return &pb.GrantAddrFSResponse{Data: srcAddrIP}, nil
}

// RevokeAddrFS ...
func (s *FileSystemAPIServer) RevokeAddrFS(ctx context.Context, r *pb.RevokeAddrFSRequest) (*pb.RevokeAddrFSResponse, error) {
	var err error
	var acctID string
	var value []byte
	var fsRef FileSysRef
	srcAddr := ""
	srcAddrIP := ""

	// Get incomming ip
	pr, ok := peer.FromContext(ctx)
	if ok {
		srcAddr = pr.Addr.String()
	}
	// Validate Token
	acctID, err = s.validateToken(r.Token)
	if err != nil {
		s.log.Info("REVOKE FAILED", zap.String("src", srcAddr), zap.String("error", "PermissionDenied"))
		return nil, errf(codes.PermissionDenied, "%v", "Invalid Token")
	}
	log := s.log.With(zap.String("src", srcAddr), zap.String("acct", acctID), zap.String("fsid", r.FSid))
	// Validate Token/Account owns this file system
	// Read FileSysRef entry to determine if it exists
	pKey := fmt.Sprintf("/fs")
	pKeyA, pKeyB := murmur3.Sum128([]byte(pKey))
	cKeyA, cKeyB := murmur3.Sum128([]byte(r.FSid))
	_, value, err = s.gstore.Read(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, nil)
	if store.IsNotFound(err) {
		log.Info("REVOKE FAILED", zap.String("error", "IDNotFound"))
		return nil, errf(codes.NotFound, "%v", "File System ID Not Found")
	}
	if err != nil {
		log.Error("REVOKE FAILED", zap.Error(err))
		return nil, errf(codes.Internal, "%v", err)
	}
	err = json.Unmarshal(value, &fsRef)
	if err != nil {
		log.Error("REVOKE FAILED", zap.Error(err))
		return nil, errf(codes.Internal, "%v", err)
	}
	if fsRef.AcctID != acctID {
		log.Info("REVOKE FAILED", zap.String("error", "AccountMismatch"), zap.String("acct2", fsRef.AcctID))
		return nil, errf(codes.FailedPrecondition, "%v", "Account Mismatch")
	}

	// REVOKE an file system entry for the addr
	// 		delete /fs/FSID/addr			addr						AddrRef
	if r.Addr == "" {
		srcAddrIP = strings.Split(srcAddr, ":")[0]
	} else {
		srcAddrIP = r.Addr
	}
	pKey = fmt.Sprintf("/fs/%s/addr", r.FSid)
	pKeyA, pKeyB = murmur3.Sum128([]byte(pKey))
	cKeyA, cKeyB = murmur3.Sum128([]byte(srcAddrIP))
	timestampMicro := brimtime.TimeToUnixMicro(time.Now())
	_, err = s.gstore.Delete(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, timestampMicro)
	if store.IsNotFound(err) {
		log.Info("REVOKE FAILED", zap.String("error", "IDNotFound"))
		return nil, errf(codes.NotFound, "%v", "File System ID Not Found")
	}
	if err != nil {
		log.Error("REVOKE FAILED", zap.Error(err))
		return nil, errf(codes.Internal, "%v", err)
	}

	// return Addr was revoked
	// Log Operation
	log.Info("REVOKE", zap.String("addr", srcAddrIP))
	return &pb.RevokeAddrFSResponse{Data: srcAddrIP}, nil
}

// ValidateResponse ...
type ValidateResponse struct {
	Token struct {
		Project struct {
			ID string `json:"id"`
		} `json:"project"`
	} `json:"token"`
}

// get server token used to validate client tokens
func auth(authURL string, username string, password string) (string, error) {
	body := fmt.Sprintf(`{"auth":{"identity":{"methods":["password"],"password":{"user":{
		"domain":{"id":"default"},"name":"%s","password":"%s"}}}}}`, username, password)
	rbody := strings.NewReader(body)
	req, err := http.NewRequest("POST", authURL+"/v3/auth/tokens", rbody)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 201 {
		return "", fmt.Errorf("Failed to acquire server auth token")
	}

	token := resp.Header.Get("X-Subject-Token")

	return token, nil
}

// validateToken ...
func (s *FileSystemAPIServer) validateToken(token string) (string, error) {
	if s.skipAuth {
		// Running in dev mode
		return "11", nil
	}
	auth_token, err := auth(s.authUrl, s.authUser, s.authPassword)
	if err != nil {
		return "", err
	}

	req, err := http.NewRequest("GET", s.authUrl+"/v3/auth/tokens", nil)

	if err != nil {
		return "", err
	}
	req.Header.Set("X-Auth-Token", auth_token)
	req.Header.Set("X-Subject-Token", token)

	resp, err := http.DefaultClient.Do(req) // TODO: Is this safe for formic?
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return "", errors.New("Invalid Token")
	}

	// parse tenant from response
	var validateResp ValidateResponse
	r, _ := ioutil.ReadAll(resp.Body)
	json.Unmarshal(r, &validateResp)
	project := validateResp.Token.Project.ID

	return project, nil
}

// deleteEntry Deletes and entry in the group store and doesn't care if
// its not Found
func (s *FileSystemAPIServer) deleteEntry(pKey string, cKey string) error {
	timestampMicro := brimtime.TimeToUnixMicro(time.Now())
	pKeyA, pKeyB := murmur3.Sum128([]byte(pKey))
	cKeyA, cKeyB := murmur3.Sum128([]byte(cKey))
	_, err := s.gstore.Delete(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, timestampMicro)
	if store.IsNotFound(err) || err == nil {
		return nil
	}
	return err
}
