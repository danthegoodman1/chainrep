package grpcx

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	grpcproto "github.com/danthegoodman1/chainrep/proto/chainrep/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type ClientAuthMode string

const (
	ClientAuthModeDisabled         ClientAuthMode = "disabled"
	ClientAuthModeVerifyIfGiven    ClientAuthMode = "verify-if-given"
	ClientAuthModeRequireAndVerify ClientAuthMode = "require-and-verify"
)

type ClientTLSConfig struct {
	CAFile     string
	CertFile   string
	KeyFile    string
	ServerName string
}

type ServerTLSConfig struct {
	CAFile                string
	CertFile              string
	KeyFile               string
	ClientAuth            ClientAuthMode
	CoordinatorIdentities []string
	StorageNodeIdentities []string
}

type rpcPlane string

const (
	rpcPlaneClientData        rpcPlane = "client_data"
	rpcPlaneCoordinatorAdmin  rpcPlane = "coordinator_admin"
	rpcPlaneCoordinatorReport rpcPlane = "coordinator_report"
	rpcPlaneStorageControl    rpcPlane = "storage_control"
	rpcPlaneStorageReplica    rpcPlane = "storage_replication"
)

type rpcAuthorizer struct {
	coordinatorIdentities map[string]struct{}
	storageIdentities     map[string]struct{}
}

func newClientTransportCredentials(cfg ClientTLSConfig) (credentials.TransportCredentials, error) {
	if err := validateClientTLSConfig(cfg); err != nil {
		return nil, err
	}
	rootCAs, err := loadCertPool(cfg.CAFile)
	if err != nil {
		return nil, err
	}
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    rootCAs,
		ServerName: cfg.ServerName,
	}
	if cfg.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client key pair: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	return credentials.NewTLS(tlsConfig), nil
}

func newServerTransportCredentials(cfg ServerTLSConfig) (credentials.TransportCredentials, error) {
	if err := validateServerTLSConfig(cfg); err != nil {
		return nil, err
	}
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load server key pair: %w", err)
	}
	tlsConfig := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
		ClientAuth:   mapClientAuthMode(cfg.ClientAuth),
	}
	if cfg.ClientAuth != ClientAuthModeDisabled {
		clientCAs, err := loadCertPool(cfg.CAFile)
		if err != nil {
			return nil, err
		}
		tlsConfig.ClientCAs = clientCAs
	}
	return credentials.NewTLS(tlsConfig), nil
}

func validateClientTLSConfig(cfg ClientTLSConfig) error {
	if cfg.CAFile == "" {
		return errors.New("client tls config requires ca file")
	}
	if (cfg.CertFile == "") != (cfg.KeyFile == "") {
		return errors.New("client tls config requires both cert and key when either is provided")
	}
	return nil
}

func validateServerTLSConfig(cfg ServerTLSConfig) error {
	if cfg.CertFile == "" || cfg.KeyFile == "" {
		return errors.New("server tls config requires cert and key")
	}
	switch cfg.ClientAuth {
	case "", ClientAuthModeDisabled, ClientAuthModeVerifyIfGiven, ClientAuthModeRequireAndVerify:
	default:
		return fmt.Errorf("invalid client auth mode %q", cfg.ClientAuth)
	}
	if cfg.ClientAuth == "" {
		cfg.ClientAuth = ClientAuthModeVerifyIfGiven
	}
	if cfg.ClientAuth != ClientAuthModeDisabled && cfg.CAFile == "" {
		return errors.New("server tls config requires ca file when client auth is enabled")
	}
	return nil
}

func mapClientAuthMode(mode ClientAuthMode) tls.ClientAuthType {
	switch mode {
	case ClientAuthModeDisabled:
		return tls.NoClientCert
	case ClientAuthModeRequireAndVerify:
		return tls.RequireAndVerifyClientCert
	case "", ClientAuthModeVerifyIfGiven:
		return tls.VerifyClientCertIfGiven
	default:
		return tls.VerifyClientCertIfGiven
	}
}

func loadCertPool(path string) (*x509.CertPool, error) {
	pemBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read ca file %s: %w", path, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, fmt.Errorf("append certs from %s: no certificates found", path)
	}
	return pool, nil
}

func newRPCAuthorizer(cfg ServerTLSConfig) *rpcAuthorizer {
	return &rpcAuthorizer{
		coordinatorIdentities: identitySet(cfg.CoordinatorIdentities),
		storageIdentities:     identitySet(cfg.StorageNodeIdentities),
	}
}

func identitySet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		out[value] = struct{}{}
	}
	return out
}

func (a *rpcAuthorizer) unaryInterceptor(policy func(string) rpcPlane) grpc.UnaryServerInterceptor {
	if a == nil {
		return nil
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := a.authorizePlane(ctx, policy(info.FullMethod)); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

func (a *rpcAuthorizer) streamInterceptor(policy func(string) rpcPlane) grpc.StreamServerInterceptor {
	if a == nil {
		return nil
	}
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := a.authorizePlane(stream.Context(), policy(info.FullMethod)); err != nil {
			return err
		}
		return handler(srv, stream)
	}
}

func (a *rpcAuthorizer) authorizePlane(ctx context.Context, plane rpcPlane) error {
	switch plane {
	case rpcPlaneStorageControl:
		return a.requireIdentityInSet(ctx, a.coordinatorIdentities, "coordinator identity required")
	case rpcPlaneStorageReplica, rpcPlaneCoordinatorReport:
		return a.requireIdentityInSet(ctx, a.storageIdentities, "storage-node identity required")
	default:
		return nil
	}
}

func (a *rpcAuthorizer) requireStorageIdentityMatch(ctx context.Context, claimed string) error {
	identity, err := a.requireIdentity(ctx)
	if err != nil {
		return err
	}
	if _, ok := a.storageIdentities[identity]; !ok {
		return status.Error(codes.PermissionDenied, "peer identity is not an authorized storage node")
	}
	if claimed != "" && claimed != identity {
		return status.Error(codes.PermissionDenied, "peer identity does not match claimed node id")
	}
	return nil
}

func (a *rpcAuthorizer) requireIdentityInSet(ctx context.Context, allowed map[string]struct{}, missingMsg string) error {
	identity, err := a.requireIdentity(ctx)
	if err != nil {
		return err
	}
	if len(allowed) == 0 {
		return status.Error(codes.PermissionDenied, missingMsg)
	}
	if _, ok := allowed[identity]; !ok {
		return status.Error(codes.PermissionDenied, "peer identity is not authorized for this rpc")
	}
	return nil
}

func (a *rpcAuthorizer) requireIdentity(ctx context.Context) (string, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "missing peer context")
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "missing tls peer identity")
	}
	if len(tlsInfo.State.VerifiedChains) == 0 || len(tlsInfo.State.PeerCertificates) == 0 {
		return "", status.Error(codes.Unauthenticated, "client certificate required")
	}
	identity, err := certificateIdentity(tlsInfo.State.PeerCertificates[0])
	if err != nil {
		return "", status.Error(codes.PermissionDenied, err.Error())
	}
	return identity, nil
}

func certificateIdentity(cert *x509.Certificate) (string, error) {
	if cert == nil {
		return "", errors.New("missing peer certificate")
	}
	if len(cert.DNSNames) > 0 && cert.DNSNames[0] != "" {
		return cert.DNSNames[0], nil
	}
	if len(cert.IPAddresses) > 0 {
		return cert.IPAddresses[0].String(), nil
	}
	if len(cert.URIs) > 0 && cert.URIs[0] != nil {
		return cert.URIs[0].String(), nil
	}
	if cert.Subject.CommonName != "" {
		return cert.Subject.CommonName, nil
	}
	return "", errors.New("peer certificate does not contain a usable identity")
}

func coordinatorRPCPlane(fullMethod string) rpcPlane {
	switch fullMethod {
	case "/chainrep.v1.CoordinatorService/ReportReplicaReady",
		"/chainrep.v1.CoordinatorService/ReportReplicaRemoved",
		"/chainrep.v1.CoordinatorService/ReportNodeHeartbeat",
		"/chainrep.v1.CoordinatorService/ReportNodeRecovered":
		return rpcPlaneCoordinatorReport
	default:
		return rpcPlaneCoordinatorAdmin
	}
}

func storageRPCPlane(fullMethod string) rpcPlane {
	switch fullMethod {
	case "/chainrep.v1.StorageService/AddReplicaAsTail",
		"/chainrep.v1.StorageService/ActivateReplica",
		"/chainrep.v1.StorageService/MarkReplicaLeaving",
		"/chainrep.v1.StorageService/RemoveReplica",
		"/chainrep.v1.StorageService/UpdateChainPeers",
		"/chainrep.v1.StorageService/ResumeRecoveredReplica",
		"/chainrep.v1.StorageService/RecoverReplica",
		"/chainrep.v1.StorageService/DropRecoveredReplica":
		return rpcPlaneStorageControl
	case "/chainrep.v1.StorageService/ForwardWrite",
		"/chainrep.v1.StorageService/CommitWrite",
		"/chainrep.v1.StorageService/FetchSnapshot",
		"/chainrep.v1.StorageService/FetchCommittedSequence":
		return rpcPlaneStorageReplica
	default:
		return rpcPlaneClientData
	}
}

func claimNodeID(req any) string {
	switch typed := req.(type) {
	case *grpcproto.ReplicaReadyReport:
		return typed.NodeId
	case *grpcproto.ReplicaRemovedReport:
		return typed.NodeId
	case *grpcproto.NodeStatus:
		return typed.NodeId
	case *grpcproto.NodeRecoveryReport:
		return typed.NodeId
	case *grpcproto.ForwardWriteRequest:
		return typed.FromNodeId
	case *grpcproto.CommitWriteRequest:
		return typed.FromNodeId
	default:
		return ""
	}
}
