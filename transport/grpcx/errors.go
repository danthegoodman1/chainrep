package grpcx

import (
	"context"
	"errors"
	"fmt"

	"github.com/danthegoodman1/chainrep/coordserver"
	"github.com/danthegoodman1/chainrep/storage"
	grpcproto "github.com/danthegoodman1/chainrep/proto/chainrep/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func encodeError(err error) error {
	if err == nil {
		return nil
	}

	var routing *storage.RoutingMismatchError
	if errors.As(err, &routing) {
		st, _ := status.New(codes.FailedPrecondition, err.Error()).WithDetails(&grpcproto.RoutingMismatchDetail{
			Slot:                 int32(routing.Slot),
			ExpectedChainVersion: routing.ExpectedChainVersion,
			CurrentChainVersion:  routing.CurrentChainVersion,
			CurrentRole:          string(routing.CurrentRole),
			CurrentState:         string(routing.CurrentState),
			Reason:               string(routing.Reason),
		})
		return st.Err()
	}

	var ambiguous *storage.AmbiguousWriteError
	if errors.As(err, &ambiguous) {
		code := codes.Unknown
		switch {
		case errors.Is(err, context.Canceled):
			code = codes.Canceled
		case errors.Is(err, context.DeadlineExceeded), errors.Is(err, storage.ErrWriteTimeout):
			code = codes.DeadlineExceeded
		}
		st, _ := status.New(code, err.Error()).WithDetails(&grpcproto.AmbiguousWriteDetail{
			Slot:                 int32(ambiguous.Slot),
			Kind:                 string(ambiguous.Kind),
			ExpectedChainVersion: ambiguous.ExpectedChainVersion,
			Cause:                ambiguous.Cause.Error(),
		})
		return st.Err()
	}

	var pressure *storage.BackpressureError
	if errors.As(err, &pressure) {
		st, _ := status.New(codes.ResourceExhausted, err.Error()).WithDetails(&grpcproto.BackpressureDetail{
			Slot:     int32(pressure.Slot),
			Current:  int32(pressure.Current),
			Limit:    int32(pressure.Limit),
			Resource: string(pressure.Resource),
			Cause:    pressure.Cause.Error(),
		})
		return st.Err()
	}

	if detail, ok := domainErrorDetail(err); ok {
		st, _ := status.New(domainErrorCode(detail.Kind), err.Error()).WithDetails(detail)
		return st.Err()
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, err.Error())
	}
	return status.Error(codes.Unknown, err.Error())
}

func decodeError(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	for _, detail := range st.Details() {
		switch typed := detail.(type) {
		case *grpcproto.RoutingMismatchDetail:
			return &storage.RoutingMismatchError{
				Slot:                 int(typed.Slot),
				ExpectedChainVersion: typed.ExpectedChainVersion,
				CurrentChainVersion:  typed.CurrentChainVersion,
				CurrentRole:          storage.ReplicaRole(typed.CurrentRole),
				CurrentState:         storage.ReplicaState(typed.CurrentState),
				Reason:               storage.RoutingMismatchReason(typed.Reason),
			}
		case *grpcproto.AmbiguousWriteDetail:
			cause := fmt.Errorf("%w: %w", storage.ErrWriteTimeout, context.DeadlineExceeded)
			if st.Code() == codes.Canceled {
				cause = fmt.Errorf("%w: %w", storage.ErrWriteTimeout, context.Canceled)
			}
			return &storage.AmbiguousWriteError{
				Slot:                 int(typed.Slot),
				Kind:                 storage.OperationKind(typed.Kind),
				ExpectedChainVersion: typed.ExpectedChainVersion,
				Cause:                cause,
			}
		case *grpcproto.BackpressureDetail:
			return &storage.BackpressureError{
				Slot:     int(typed.Slot),
				Current:  int(typed.Current),
				Limit:    int(typed.Limit),
				Resource: storage.BackpressureResource(typed.Resource),
				Cause:    backpressureCause(storage.BackpressureResource(typed.Resource)),
			}
		case *grpcproto.DomainErrorDetail:
			return decodeDomainError(typed)
		}
	}
	switch st.Code() {
	case codes.DeadlineExceeded:
		return context.DeadlineExceeded
	case codes.Canceled:
		return context.Canceled
	}
	return err
}

func domainErrorDetail(err error) (*grpcproto.DomainErrorDetail, bool) {
	switch {
	case errors.Is(err, storage.ErrUnknownReplica):
		return &grpcproto.DomainErrorDetail{Kind: "unknown_replica"}, true
	case errors.Is(err, storage.ErrWriteRejected):
		return &grpcproto.DomainErrorDetail{Kind: "write_rejected"}, true
	case errors.Is(err, storage.ErrSequenceMismatch):
		return &grpcproto.DomainErrorDetail{Kind: "sequence_mismatch"}, true
	case errors.Is(err, storage.ErrPeerMismatch):
		return &grpcproto.DomainErrorDetail{Kind: "peer_mismatch"}, true
	case errors.Is(err, storage.ErrStateMismatch):
		return &grpcproto.DomainErrorDetail{Kind: "storage_state_mismatch"}, true
	case errors.Is(err, storage.ErrProtocolConflict):
		return &grpcproto.DomainErrorDetail{Kind: "protocol_conflict"}, true
	case errors.Is(err, storage.ErrInvalidTransition):
		return &grpcproto.DomainErrorDetail{Kind: "invalid_transition"}, true
	case errors.Is(err, coordserver.ErrUnexpectedProgress):
		return &grpcproto.DomainErrorDetail{Kind: "unexpected_progress"}, true
	case errors.Is(err, coordserver.ErrStateMismatch):
		return &grpcproto.DomainErrorDetail{Kind: "coordserver_state_mismatch"}, true
	default:
		return nil, false
	}
}

func domainErrorCode(kind string) codes.Code {
	switch kind {
	case "unknown_replica":
		return codes.NotFound
	default:
		return codes.FailedPrecondition
	}
}

func decodeDomainError(detail *grpcproto.DomainErrorDetail) error {
	switch detail.Kind {
	case "unknown_replica":
		return storage.ErrUnknownReplica
	case "write_rejected":
		return storage.ErrWriteRejected
	case "sequence_mismatch":
		return storage.ErrSequenceMismatch
	case "peer_mismatch":
		return storage.ErrPeerMismatch
	case "storage_state_mismatch":
		return storage.ErrStateMismatch
	case "protocol_conflict":
		return storage.ErrProtocolConflict
	case "invalid_transition":
		return storage.ErrInvalidTransition
	case "unexpected_progress":
		return coordserver.ErrUnexpectedProgress
	case "coordserver_state_mismatch":
		return coordserver.ErrStateMismatch
	default:
		return status.Error(codes.Unknown, detail.Kind)
	}
}

func backpressureCause(resource storage.BackpressureResource) error {
	switch resource {
	case storage.BackpressureResourceClientWrite:
		return storage.ErrWriteBackpressure
	case storage.BackpressureResourceReplicaBuffer:
		return storage.ErrReplicaBackpressure
	case storage.BackpressureResourceCatchup:
		return storage.ErrCatchupBackpressure
	default:
		return storage.ErrWriteBackpressure
	}
}
