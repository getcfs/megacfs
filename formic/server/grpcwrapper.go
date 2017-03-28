package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/getcfs/megacfs/formic/formicproto"
	"github.com/gholt/store"
	"golang.org/x/net/context"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

type grpcWrapper struct {
	fs           *oortFS
	validIPs     map[string]map[string]time.Time
	comms        *storeComms
	skipAuth     bool
	authURL      string
	authUser     string
	authPassword string
}

func newGRPCWrapper(fs *oortFS, comms *storeComms, skipAuth bool, authURL string, authUser string, authPassword string) *grpcWrapper {
	return &grpcWrapper{
		fs:           fs,
		validIPs:     make(map[string]map[string]time.Time),
		comms:        comms,
		skipAuth:     skipAuth,
		authURL:      authURL,
		authUser:     authUser,
		authPassword: authPassword,
	}
}

func (g *grpcWrapper) Check(stream formicproto.Formic_CheckServer) error {
	// NOTE: Each of these streams is synchronized req1, resp1, req2, resp2.
	// But it doesn't have to be that way, it was just simpler to code. Each
	// client/server pair will have a stream for each request/response type, so
	// there's a pretty good amount of concurrency going on there already.
	// Perhaps later we can experiment with intrastream concurrency and see if
	// the complexity is worth it.
	//
	// The main reason for using streams over unary grpc requests was
	// benchmarked speed gains. I suspect it is because unary requests actually
	// set up and tear down streams for each request, but that's just a guess.
	// We stopped looking into it once we noticed the speed gains from
	// switching to streaming.
	var resp formicproto.CheckResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		var fsid string
		if fsid, err = g.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = g.fs.Check(stream.Context(), req, &resp, fsid); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (g *grpcWrapper) CreateFS(stream formicproto.Formic_CreateFSServer) error {
	var resp formicproto.CreateFSResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		acctID, err := g.validateToken(req.Token)
		if err != nil {
			resp.Err = err.Error()
		} else if err = g.fs.CreateFS(stream.Context(), req, &resp, acctID); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (g *grpcWrapper) Create(stream formicproto.Formic_CreateServer) error {
	var resp formicproto.CreateResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		var fsid string
		if fsid, err = g.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = g.fs.Create(stream.Context(), req, &resp, fsid); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (g *grpcWrapper) DeleteFS(stream formicproto.Formic_DeleteFSServer) error {
	var resp formicproto.DeleteFSResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		acctID, err := g.validateToken(req.Token)
		if err != nil {
			resp.Err = err.Error()
		} else if err = g.fs.DeleteFS(stream.Context(), req, &resp, acctID); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (g *grpcWrapper) GetAttr(stream formicproto.Formic_GetAttrServer) error {
	var resp formicproto.GetAttrResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		var fsid string
		if fsid, err = g.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = g.fs.GetAttr(stream.Context(), req, &resp, fsid); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (g *grpcWrapper) Getxattr(stream formicproto.Formic_GetxattrServer) error {
	var resp formicproto.GetxattrResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		var fsid string
		if fsid, err = g.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = g.fs.Getxattr(stream.Context(), req, &resp, fsid); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (g *grpcWrapper) GrantAddrFS(stream formicproto.Formic_GrantAddrFSServer) error {
	var resp formicproto.GrantAddrFSResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		acctID, err := g.validateToken(req.Token)
		if err != nil {
			resp.Err = err.Error()
		} else if err = g.fs.GrantAddrFS(stream.Context(), req, &resp, acctID); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (g *grpcWrapper) InitFs(stream formicproto.Formic_InitFsServer) error {
	var resp formicproto.InitFsResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		var fsid string
		if fsid, err = g.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = g.fs.InitFs(stream.Context(), req, &resp, fsid); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (g *grpcWrapper) ListFS(stream formicproto.Formic_ListFSServer) error {
	var resp formicproto.ListFSResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		acctID, err := g.validateToken(req.Token)
		if err != nil {
			resp.Err = err.Error()
		} else if err = g.fs.ListFS(stream.Context(), req, &resp, acctID); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (g *grpcWrapper) Listxattr(stream formicproto.Formic_ListxattrServer) error {
	var resp formicproto.ListxattrResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		var fsid string
		if fsid, err = g.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = g.fs.Listxattr(stream.Context(), req, &resp, fsid); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (g *grpcWrapper) Lookup(stream formicproto.Formic_LookupServer) error {
	var resp formicproto.LookupResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		var fsid string
		if fsid, err = g.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = g.fs.Lookup(stream.Context(), req, &resp, fsid); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (g *grpcWrapper) MkDir(stream formicproto.Formic_MkDirServer) error {
	var resp formicproto.MkDirResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		var fsid string
		if fsid, err = g.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = g.fs.MkDir(stream.Context(), req, &resp, fsid); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (g *grpcWrapper) ReadDirAll(stream formicproto.Formic_ReadDirAllServer) error {
	var resp formicproto.ReadDirAllResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		var fsid string
		if fsid, err = g.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = g.fs.ReadDirAll(stream.Context(), req, &resp, fsid); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (g *grpcWrapper) Readlink(stream formicproto.Formic_ReadlinkServer) error {
	var resp formicproto.ReadlinkResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		var fsid string
		if fsid, err = g.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = g.fs.Readlink(stream.Context(), req, &resp, fsid); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (g *grpcWrapper) Read(stream formicproto.Formic_ReadServer) error {
	var resp formicproto.ReadResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		var fsid string
		if fsid, err = g.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = g.fs.Read(stream.Context(), req, &resp, fsid); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (g *grpcWrapper) Remove(stream formicproto.Formic_RemoveServer) error {
	var resp formicproto.RemoveResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		var fsid string
		if fsid, err = g.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = g.fs.Remove(stream.Context(), req, &resp, fsid); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (g *grpcWrapper) Removexattr(stream formicproto.Formic_RemovexattrServer) error {
	var resp formicproto.RemovexattrResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		var fsid string
		if fsid, err = g.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = g.fs.Removexattr(stream.Context(), req, &resp, fsid); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (g *grpcWrapper) Rename(stream formicproto.Formic_RenameServer) error {
	var resp formicproto.RenameResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		var fsid string
		if fsid, err = g.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = g.fs.Rename(stream.Context(), req, &resp, fsid); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (g *grpcWrapper) RevokeAddrFS(stream formicproto.Formic_RevokeAddrFSServer) error {
	var resp formicproto.RevokeAddrFSResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		acctID, err := g.validateToken(req.Token)
		if err != nil {
			resp.Err = err.Error()
		} else if err = g.fs.RevokeAddrFS(stream.Context(), req, &resp, acctID); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (g *grpcWrapper) SetAttr(stream formicproto.Formic_SetAttrServer) error {
	var resp formicproto.SetAttrResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		var fsid string
		if fsid, err = g.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = g.fs.SetAttr(stream.Context(), req, &resp, fsid); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (g *grpcWrapper) Setxattr(stream formicproto.Formic_SetxattrServer) error {
	var resp formicproto.SetxattrResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		var fsid string
		if fsid, err = g.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = g.fs.Setxattr(stream.Context(), req, &resp, fsid); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (g *grpcWrapper) ShowFS(stream formicproto.Formic_ShowFSServer) error {
	var resp formicproto.ShowFSResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		acctID, err := g.validateToken(req.Token)
		if err != nil {
			resp.Err = err.Error()
		} else if err = g.fs.ShowFS(stream.Context(), req, &resp, acctID); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (g *grpcWrapper) Statfs(stream formicproto.Formic_StatfsServer) error {
	var resp formicproto.StatfsResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		var fsid string
		if fsid, err = g.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = g.fs.Statfs(stream.Context(), req, &resp, fsid); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (g *grpcWrapper) Symlink(stream formicproto.Formic_SymlinkServer) error {
	var resp formicproto.SymlinkResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		var fsid string
		if fsid, err = g.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = g.fs.Symlink(stream.Context(), req, &resp, fsid); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (g *grpcWrapper) UpdateFS(stream formicproto.Formic_UpdateFSServer) error {
	var resp formicproto.UpdateFSResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		acctID, err := g.validateToken(req.Token)
		if err != nil {
			resp.Err = err.Error()
		} else if err = g.fs.UpdateFS(stream.Context(), req, &resp, acctID); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (g *grpcWrapper) Write(stream formicproto.Formic_WriteServer) error {
	var resp formicproto.WriteResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		var fsid string
		if fsid, err = g.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = g.fs.Write(stream.Context(), req, &resp, fsid); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

// validateIP returns the FSID for the context or an error.
func (g *grpcWrapper) validateIP(ctx context.Context) (string, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return "", errors.New("couldn't get client ip")
	}
	ip, _, err := net.SplitHostPort(p.Addr.String())
	if err != nil {
		return "", err
	}
	md, ok := metadata.FromContext(ctx)
	if !ok {
		return "", errors.New("no metadata sent")
	}
	fsidMetadata, ok := md["fsid"]
	if !ok {
		return "", errors.New("file system id not sent")
	}
	fsid := fsidMetadata[0]
	ips, ok := g.validIPs[fsid]
	if !ok {
		ips = make(map[string]time.Time)
		g.validIPs[fsid] = ips
	}
	cacheTime, ok := ips[ip]
	if ok && cacheTime.After(time.Now()) {
		return fsid, nil
	}
	_, err = g.comms.ReadGroupItem(ctx, []byte(fmt.Sprintf("/fs/%s/addr", fsid)), []byte(ip))
	if store.IsNotFound(err) {
		return "", errors.New("permission denied")
	}
	if err != nil {
		return "", err
	}
	g.validIPs[fsid][ip] = time.Now().Add(time.Second * time.Duration(180.0+180.0*rand.NormFloat64()*0.1))
	return fsid, nil
}

type validateTokenResponse struct {
	Token struct {
		Project struct {
			ID string `json:"id"`
		} `json:"project"`
	} `json:"token"`
}

// validateToken ensure the token is valid and returns the Account ID or an
// error.
func (g *grpcWrapper) validateToken(token string) (string, error) {
	if g.skipAuth {
		return "11", nil
	}
	serverAuthToken, err := serverAuth(g.authURL, g.authUser, g.authPassword)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequest("GET", g.authURL+"/v3/auth/tokens", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("X-Auth-Token", serverAuthToken)
	req.Header.Set("X-Subject-Token", token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("token validation gave status %d", resp.StatusCode)
	}
	var validateResp validateTokenResponse
	r, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if err = json.Unmarshal(r, &validateResp); err != nil {
		return "", err
	}
	return validateResp.Token.Project.ID, nil
}

// serverAuth return the X-Auth-Token to use or an error.
func serverAuth(url string, user string, password string) (string, error) {
	body := fmt.Sprintf(`{"auth":{"identity":{"methods":["password"],"password":{"user":{"domain":{"id":"default"},"name":"%s","password":"%s"}}}}}`, user, password)
	rbody := strings.NewReader(body)
	req, err := http.NewRequest("POST", url+"/v3/auth/tokens", rbody)
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
		return "", fmt.Errorf("server auth token request gave status %d", resp.StatusCode)
	}
	return resp.Header.Get("X-Subject-Token"), nil
}
