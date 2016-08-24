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

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"time"

	pb "github.com/getcfs/megacfs/formic/proto"
	"github.com/gholt/brimtime"
	"github.com/gholt/store"
	"github.com/prometheus/common/log"
	uuid "github.com/satori/go.uuid"
	"github.com/spaolacci/murmur3"
	"github.com/uber-go/zap"
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
	gstore store.GroupStore
	log    zap.Logger
}

// FSAttrList ...
var FSAttrList = []string{"name"}

// NewFileSystemAPIServer ...
func NewFileSystemAPIServer(store store.GroupStore, logger zap.Logger) *FileSystemAPIServer {
	s := &FileSystemAPIServer{
		gstore: store,
		log:    logger,
	}

	return s
}

// CreateFS ...
func (s *FileSystemAPIServer) CreateFS(ctx context.Context, r *pb.CreateFSRequest) (*pb.CreateFSResponse, error) {
	var err error
	var acctID string
	srcAddr := ""
	var fsRef FileSysRef
	var fsRefByte []byte
	var fsSysAttr FileSysAttr
	var fsSysAttrByte []byte

	// Get incomming ip
	pr, ok := peer.FromContext(ctx)
	if ok {
		srcAddr = pr.Addr.String()
	}

	// Validate Token
	acctID, err = s.validateToken(r.Token)
	if err != nil {
		s.log.Info("CREATE FAILED", zap.String("src", srcAddr), zap.String("error", "PermissionDenied"))
		return nil, errf(codes.PermissionDenied, "%v", "Invalid Token")
	}
	log := s.log.With(zap.String("src", srcAddr), zap.String("acct", acctID))

	fsID := uuid.NewV4().String()
	timestampMicro := brimtime.TimeToUnixMicro(time.Now())
	// Write file system reference entries.
	// write /fs 								FSID						FileSysRef
	pKey := "/fs"
	pKeyA, pKeyB := murmur3.Sum128([]byte(pKey))
	cKeyA, cKeyB := murmur3.Sum128([]byte(fsID))
	fsRef.AcctID = acctID
	fsRef.FSID = fsID
	fsRefByte, err = json.Marshal(fsRef)
	if err != nil {
		log.Error("CREATE FAILED", zap.Error(err))
		return nil, errf(codes.Internal, "%v", err)
	}
	_, err = s.gstore.Write(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, timestampMicro, fsRefByte)
	if err != nil {
		log.Error("CREATE FAILED", zap.Error(err))
		return nil, errf(codes.Internal, "%v", err)
	}
	// write /acct/acctID				FSID						FileSysRef
	pKey = fmt.Sprintf("/acct/%s", acctID)
	pKeyA, pKeyB = murmur3.Sum128([]byte(pKey))
	_, err = s.gstore.Write(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, timestampMicro, fsRefByte)
	if err != nil {
		log.Error("CREATE FAILED", zap.Error(err))
		return nil, errf(codes.Internal, "%v", err)
	}
	// Write file system attributes
	// write /fs/FSID						name						FileSysAttr
	pKey = fmt.Sprintf("/fs/%s", fsID)
	pKeyA, pKeyB = murmur3.Sum128([]byte(pKey))
	cKeyA, cKeyB = murmur3.Sum128([]byte("name"))
	fsSysAttr.Attr = "name"
	fsSysAttr.Value = r.FSName
	fsSysAttr.FSID = fsID
	fsSysAttrByte, err = json.Marshal(fsSysAttr)
	if err != nil {
		log.Error("CREATE FAILED", zap.Error(err))
		return nil, errf(codes.Internal, "%v", err)
	}
	_, err = s.gstore.Write(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, timestampMicro, fsSysAttrByte)
	if err != nil {
		log.Error("CREATE FAILED", zap.Error(err))
		return nil, errf(codes.Internal, "%v", err)
	}

	// Return File System UUID
	// Log Operation
	log.Info("CREATE", zap.String("fsid", fsID))
	return &pb.CreateFSResponse{Data: fsID}, nil
}

// ShowFS ...
func (s *FileSystemAPIServer) ShowFS(ctx context.Context, r *pb.ShowFSRequest) (*pb.ShowFSResponse, error) {
	var err error
	var acctID string
	srcAddr := ""

	// Get incomming ip
	pr, ok := peer.FromContext(ctx)
	if ok {
		srcAddr = pr.Addr.String()
	}

	// Validate Token
	acctID, err = s.validateToken(r.Token)
	if err != nil {
		s.log.Info("SHOW FAILED", zap.String("src", srcAddr), zap.String("error", "PermissionDenied"))
		return nil, errf(codes.PermissionDenied, "%v", "Invalid Token")
	}
	log := s.log.With(zap.String("src", srcAddr), zap.String("acct", acctID), zap.String("fsid", r.FSid))

	var fs FileSysMeta
	var value []byte
	var fsRef FileSysRef
	var addrData AddrRef
	var fsAttrData FileSysAttr
	var aList []string
	fs.ID = r.FSid

	// Read FileSysRef entry to determine if it exists
	pKey := fmt.Sprintf("/fs")
	pKeyA, pKeyB := murmur3.Sum128([]byte(pKey))
	cKeyA, cKeyB := murmur3.Sum128([]byte(fs.ID))
	_, value, err = s.gstore.Read(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, nil)
	if store.IsNotFound(err) {
		log.Info("SHOW FAILED", zap.String("error", "IDNotFound"))
		return nil, errf(codes.NotFound, "%v", "File System ID Not Found")
	}
	if err != nil {
		log.Error("SHOW FAILED", zap.Error(err))
		return nil, errf(codes.Internal, "%v", err)
	}
	err = json.Unmarshal(value, &fsRef)
	if err != nil {
		log.Error("SHOW FAILED", zap.Error(err))
		return nil, errf(codes.Internal, "%v", err)
	}

	// Validate Token/Account own the file system
	if fsRef.AcctID != acctID {
		log.Info("SHOW FAILED", zap.String("error", "AccountMismatch"), zap.String("acct2", fsRef.AcctID))
		return nil, errf(codes.FailedPrecondition, "%v", "Account Mismatch")
	}
	fs.AcctID = fsRef.AcctID

	// Read the file system attributes
	// group-lookup /fs			FSID
	//		Iterate over all the atributes
	pKey = fmt.Sprintf("/fs/%s", fs.ID)
	pKeyA, pKeyB = murmur3.Sum128([]byte(pKey))
	cKeyA, cKeyB = murmur3.Sum128([]byte("name"))
	_, value, err = s.gstore.Read(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, nil)
	if store.IsNotFound(err) {
		log.Info("SHOW FAILED", zap.String("error", "NameNotFound"))
		return nil, errf(codes.NotFound, "%v", "File System Name Not Found")
	}
	if err != nil {
		log.Error("SHOW FAILED", zap.Error(err))
		return nil, errf(codes.Internal, "%v", err)
	}
	err = json.Unmarshal(value, &fsAttrData)
	if err != nil {
		log.Error("SHOW FAILED", zap.Error(err))
		return nil, errf(codes.Internal, "%v", err)
	}
	fs.Name = fsAttrData.Value

	// Read list of granted ip addresses
	// group-lookup printf("/fs/%s/addr", FSID)
	pKey = fmt.Sprintf("/fs/%s/addr", fs.ID)
	pKeyA, pKeyB = murmur3.Sum128([]byte(pKey))
	items, err := s.gstore.ReadGroup(context.Background(), pKeyA, pKeyB)
	if !store.IsNotFound(err) {
		// No addr granted
		aList = make([]string, len(items))
		for k, v := range items {
			err = json.Unmarshal(v.Value, &addrData)
			if err != nil {
				log.Error("SHOW FAILED", zap.Error(err))
				return nil, errf(codes.Internal, "%v", err)
			}
			aList[k] = addrData.Addr
		}
	}
	if err != nil {
		log.Error("SHOW FAILED", zap.Error(err))
		return nil, errf(codes.Internal, "%v", err)
	}
	fs.Addr = aList

	// Return File System
	fsJSON, jerr := json.Marshal(&fs)
	if jerr != nil {
		return nil, errf(codes.Internal, "%s", jerr)
	}
	// Log Operation
	log.Info("SHOW")
	return &pb.ShowFSResponse{Data: string(fsJSON)}, nil
}

// ListFS ...
func (s *FileSystemAPIServer) ListFS(ctx context.Context, r *pb.ListFSRequest) (*pb.ListFSResponse, error) {
	srcAddr := ""
	// Get incomming ip
	pr, ok := peer.FromContext(ctx)
	if ok {
		srcAddr = pr.Addr.String()
	}
	// Validate Token
	acctID, err := s.validateToken(r.Token)
	if err != nil {
		s.log.Info("LIST FAILED", zap.String("src", srcAddr), zap.String("error", "PermissionDenied"))
		return nil, errf(codes.PermissionDenied, "%v", "Invalid Token")
	}
	log := s.log.With(zap.String("src", srcAddr), zap.String("acct", acctID))

	var value []byte
	var fsRef FileSysRef
	var addrData AddrRef
	var fsAttrData FileSysAttr
	var aList []string

	// Read Group /acct/acctID				_						FileSysRef
	pKey := fmt.Sprintf("/acct/%s", acctID)
	pKeyA, pKeyB := murmur3.Sum128([]byte(pKey))
	list, err := s.gstore.ReadGroup(context.Background(), pKeyA, pKeyB)
	if err != nil {
		log.Error("LIST FAILED", zap.Error(err))
		return nil, errf(codes.Internal, "%v", err)
	}
	fsList := make([]FileSysMeta, len(list))
	for k, v := range list {
		clear(&fsRef)
		clear(&addrData)
		clear(&fsAttrData)
		clear(&aList)
		err = json.Unmarshal(v.Value, &fsRef)
		if err != nil {
			log.Error("LIST FAILED", zap.Error(err))
			return nil, errf(codes.Internal, "%v", err)
		}
		fsList[k].AcctID = acctID
		fsList[k].ID = fsRef.FSID

		// Get File System Name
		pKey = fmt.Sprintf("/fs/%s", fsList[k].ID)
		pKeyA, pKeyB = murmur3.Sum128([]byte(pKey))
		cKeyA, cKeyB := murmur3.Sum128([]byte("name"))
		_, value, err = s.gstore.Read(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, nil)
		if store.IsNotFound(err) {
			log.Info("LIST FAILED", zap.String("error", "NameNotFound"), zap.String("name", fsList[k].ID))
			return nil, errf(codes.NotFound, "%v", "File System Name Not Found")
		}
		if err != nil {
			log.Error("LIST FAILED", zap.Error(err))
			return nil, errf(codes.Internal, "%v", err)
		}
		err = json.Unmarshal(value, &fsAttrData)
		if err != nil {
			log.Error("LIST FAILED", zap.Error(err))
			return nil, errf(codes.Internal, "%v", err)
		}
		fsList[k].Name = fsAttrData.Value

		// Get List of addrs
		pKey = fmt.Sprintf("/fs/%s/addr", fsList[k].ID)
		pKeyA, pKeyB = murmur3.Sum128([]byte(pKey))
		items, err := s.gstore.ReadGroup(context.Background(), pKeyA, pKeyB)
		if !store.IsNotFound(err) {
			// No addr granted
			aList = make([]string, len(items))
			for sk, sv := range items {
				err = json.Unmarshal(sv.Value, &addrData)
				if err != nil {
					log.Error("LIST FAILED", zap.Error(err))
					return nil, errf(codes.Internal, "%v", err)
				}
				aList[sk] = addrData.Addr
			}
		}
		if err != nil {
			log.Error("LIST FAILED", zap.Error(err))
			return nil, errf(codes.Internal, "%v", err)
		}
		fsList[k].Addr = aList
	}

	// Return a File System List
	fsListJSON, jerr := json.Marshal(&fsList)
	if jerr != nil {
		return nil, errf(codes.Internal, "%s", jerr)
	}
	// Log Operation
	log.Info("LIST")
	return &pb.ListFSResponse{Data: string(fsListJSON)}, nil
}

// DeleteFS ...
func (s *FileSystemAPIServer) DeleteFS(ctx context.Context, r *pb.DeleteFSRequest) (*pb.DeleteFSResponse, error) {
	var err error
	srcAddr := ""
	// Get incomming ip
	pr, ok := peer.FromContext(ctx)
	if ok {
		srcAddr = pr.Addr.String()
	}

	// validate Token
	acctID, err := s.validateToken(r.Token)
	if err != nil {
		s.log.Info("DELETE FAILED", zap.String("src", srcAddr), zap.String("error", "PermissionDenied"))
		return nil, errf(codes.PermissionDenied, "%v", "Invalid Token")
	}
	log := s.log.With(zap.String("src", srcAddr), zap.String("acct", acctID), zap.String("fsid", r.FSid))

	// Validate Token/Account own this file system

	// Prep things to return
	// Log Operation
	log.Info("DELETE NOTIMPLEMENTED")
	return &pb.DeleteFSResponse{Data: "Delete Operation not supported at this time"}, nil
}

// UpdateFS ...
func (s *FileSystemAPIServer) UpdateFS(ctx context.Context, r *pb.UpdateFSRequest) (*pb.UpdateFSResponse, error) {
	var err error
	var value []byte
	var fsRef FileSysRef
	var fsSysAttr FileSysAttr
	var fsSysAttrByte []byte
	srcAddr := ""
	// Get incomming ip
	pr, ok := peer.FromContext(ctx)
	if ok {
		srcAddr = pr.Addr.String()
	}

	if r.Filesys.Name == "" {
		log.Info("UPDATE FAILED", zap.String("error", "NameRequired"), zap.String("name", ""))
		return nil, errf(codes.FailedPrecondition, "%v", "File System name cannot be empty")
	}

	// validate Token
	acctID, err := s.validateToken(r.Token)
	if err != nil {
		s.log.Info("UPDATE FAILED", zap.String("src", srcAddr), zap.String("error", "PermissionDenied"))
		return nil, errf(codes.PermissionDenied, "%v", "Invalid Token")
	}
	log := s.log.With(zap.String("src", srcAddr), zap.String("acct", acctID), zap.String("fsid", r.FSid))

	// validate that Token/Account own this file system
	// Read FileSysRef entry to determine if it exists
	pKey := fmt.Sprintf("/fs")
	pKeyA, pKeyB := murmur3.Sum128([]byte(pKey))
	cKeyA, cKeyB := murmur3.Sum128([]byte(r.FSid))
	_, value, err = s.gstore.Read(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, nil)
	if store.IsNotFound(err) {
		log.Info("UPDATE FAILED", zap.String("error", "IDNotFound"))
		return nil, errf(codes.NotFound, "%v", "File System ID Not Found")
	}
	if err != nil {
		log.Error("UPDATE FAILED", zap.Error(err))
		return nil, errf(codes.Internal, "%v", err)
	}
	err = json.Unmarshal(value, &fsRef)
	if err != nil {
		log.Error("UPDATE FAILED", zap.Error(err))
		return nil, errf(codes.Internal, "%v", err)
	}
	if fsRef.AcctID != acctID {
		log.Info("UPDATE FAILED", zap.String("error", "AccountMismatch"), zap.String("acct", fsRef.AcctID))
		return nil, errf(codes.FailedPrecondition, "%v", "Account Mismatch")
	}

	// Write file system attributes
	// write /fs/FSID						name						FileSysAttr
	pKey = fmt.Sprintf("/fs/%s", r.FSid)
	pKeyA, pKeyB = murmur3.Sum128([]byte(pKey))
	cKeyA, cKeyB = murmur3.Sum128([]byte("name"))
	fsSysAttr.Attr = "name"
	fsSysAttr.Value = r.Filesys.Name
	fsSysAttr.FSID = r.FSid
	fsSysAttrByte, err = json.Marshal(fsSysAttr)
	if err != nil {
		log.Error("UPDATE FAILED", zap.Error(err))
		return nil, errf(codes.Internal, "%v", err)
	}
	timestampMicro := brimtime.TimeToUnixMicro(time.Now())
	_, err = s.gstore.Write(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, timestampMicro, fsSysAttrByte)
	if err != nil {
		log.Error("UPDATE FAILED", zap.Error(err))
		return nil, errf(codes.Internal, "%v", err)
	}

	// return message
	// Log Operation
	log.Info("UPDATE", zap.String("acct", r.FSid), zap.String("name", r.Filesys.Name))
	return &pb.UpdateFSResponse{Data: r.FSid}, nil
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
	pKey = fmt.Sprintf("/fs/%s/addr", r.FSid)
	pKeyA, pKeyB = murmur3.Sum128([]byte(pKey))
	cKeyA, cKeyB = murmur3.Sum128([]byte(r.Addr))
	timestampMicro := brimtime.TimeToUnixMicro(time.Now())
	addrData.Addr = r.Addr
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
	log.Info("GRANT", zap.String("addr", r.Addr))
	return &pb.GrantAddrFSResponse{Data: r.FSid}, nil
}

// RevokeAddrFS ...
func (s *FileSystemAPIServer) RevokeAddrFS(ctx context.Context, r *pb.RevokeAddrFSRequest) (*pb.RevokeAddrFSResponse, error) {
	var err error
	var acctID string
	var value []byte
	var fsRef FileSysRef
	srcAddr := ""

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
	pKey = fmt.Sprintf("/fs/%s/addr", r.FSid)
	pKeyA, pKeyB = murmur3.Sum128([]byte(pKey))
	cKeyA, cKeyB = murmur3.Sum128([]byte(r.Addr))
	timestampMicro := brimtime.TimeToUnixMicro(time.Now())
	_, err = s.gstore.Delete(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, timestampMicro)
	if store.IsNotFound(err) {
		log.Info("REVOKE FAILED", zap.String("error", "IDNotFound"))
		return nil, errf(codes.NotFound, "%v", "File System ID Not Found")
	}

	// return Addr was revoked
	// Log Operation
	log.Info("REVOKE", zap.String("addr", r.Addr))
	return &pb.RevokeAddrFSResponse{Data: r.FSid}, nil
}

type ValidateResponse struct {
	Access struct {
		Token struct {
			Tenant struct {
				ID string `json:"id"`
			} `json:"tenant"`
		} `json:"token"`
	} `json:"access"`
}

// validateToken ...
func (s *FileSystemAPIServer) validateToken(token string) (string, error) {
	url := "https://identity.api.rackspacecloud.com/v2.0/tokens/" + token
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("X-Auth-Token", token)

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
	tenant := validateResp.Access.Token.Tenant.ID

	return tenant, nil
}
