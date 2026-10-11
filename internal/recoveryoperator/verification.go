package recoveryoperator

import (
	"encoding/json"

	"github.com/cofy-x/kova/internal/service/recoverydisposal"
	"github.com/urfave/cli/v2"
)

const (
	maxVerificationPinsBytes   = 4 * 1024 * 1024
	maxVerificationRecordBytes = 2 * 1024 * 1024
)

// VerificationPins is selected outside Kova. The command never derives an
// expectation or public trust root from either incoming signed envelope.
// The whole canonical file is separately bound by --pins-digest.
type VerificationPins struct {
	Version            string                                     `json:"version"`
	Action             string                                     `json:"action"`
	Authorization      *recoverydisposal.AuthorizationExpectation `json:"authorization"`
	AuthorizationRoots []PublicKeyPin                             `json:"authorizationRoots"`
	Closure            *recoverydisposal.ClosureExpectation       `json:"closure"`
	ClosureRoots       []PublicKeyPin                             `json:"closureRoots"`
}

// VerificationReport deliberately describes local schema/signature checks,
// never the existence or truth of the cited external evidence. In particular
// a verified closure envelope is not a capacity-release or route-CAS proof.
type VerificationReport struct {
	Stage                    string                              `json:"stage"`
	ProofScope               string                              `json:"proofScope"`
	PinsDigest               string                              `json:"pinsDigest"`
	IncidentID               string                              `json:"incidentId"`
	AuthorizationDigest      string                              `json:"authorizationDigest"`
	ReceiptSetDigest         string                              `json:"receiptSetDigest"`
	ReceiptCount             int                                 `json:"receiptCount"`
	SuccessorPlanDigest      string                              `json:"successorPlanDigest"`
	ClosureDigest            string                              `json:"closureDigest,omitempty"`
	Successor                *recoverydisposal.SuccessorIdentity `json:"successor,omitempty"`
	ExternalEvidenceObserved bool                                `json:"externalEvidenceObserved"`
	IncidentClosed           bool                                `json:"incidentClosed"`
	CapacityReleased         bool                                `json:"capacityReleased"`
	SuccessorCreated         bool                                `json:"successorCreated"`
}

func verificationCommand(action string) *cli.Command {
	flags := []cli.Flag{
		&cli.StringFlag{Name: "pins", Required: true, Usage: "private canonical independent expectations and public roots"},
		&cli.StringFlag{Name: "pins-digest", Required: true, Usage: "independently selected whole-pins-file SHA-256"},
		&cli.StringFlag{Name: "authorization", Required: true, Usage: "private canonical signed authorization envelope"},
	}
	if action == "verify-closure" {
		flags = append(flags, &cli.StringFlag{Name: "closure", Required: true, Usage: "private canonical signed closure envelope"})
	}
	return &cli.Command{Name: action, Usage: "offline schema/signature verification only; never observe physical evidence or mutate", Flags: flags,
		Action: func(c *cli.Context) error { return runVerification(c, action) }}
}

func runVerification(c *cli.Context, action string) error {
	if c.NArg() != 0 {
		return ErrInput
	}
	report, err := verifyRecords(action, c.String("pins"), c.String("pins-digest"), c.String("authorization"), c.String("closure"))
	if err != nil {
		return ErrInput
	}
	if json.NewEncoder(c.App.Writer).Encode(report) != nil {
		return errOutput
	}
	return nil
}

func verifyRecords(action, pinsPath, pinsDigest, authorizationPath, closurePath string) (VerificationReport, error) {
	if !validDigest(pinsDigest) || (action != "verify-authorization" && action != "verify-closure") ||
		(action == "verify-authorization" && closurePath != "") || (action == "verify-closure" && closurePath == "") {
		return VerificationReport{}, ErrInput
	}
	raw, err := readRegularFile(pinsPath, maxVerificationPinsBytes, true)
	var pins VerificationPins
	if err != nil || digest(raw) != pinsDigest || decodeCanonical(raw, &pins) != nil ||
		pins.Version != "1" || pins.Action != action || pins.Authorization == nil {
		return VerificationReport{}, ErrInput
	}
	if action == "verify-authorization" && (pins.Closure != nil || len(pins.ClosureRoots) != 0) ||
		action == "verify-closure" && pins.Closure == nil {
		return VerificationReport{}, ErrInput
	}
	authorizationRoots, err := roots(pins.AuthorizationRoots)
	if err != nil {
		return VerificationReport{}, ErrInput
	}
	raw, err = readRegularFile(authorizationPath, maxVerificationRecordBytes, true)
	if err != nil {
		return VerificationReport{}, ErrInput
	}
	authorization, err := recoverydisposal.VerifyAuthorization(raw, *pins.Authorization, authorizationRoots)
	if err != nil {
		return VerificationReport{}, ErrInput
	}
	report := VerificationReport{Stage: "authorization-verified", ProofScope: "schema-and-signature-only; external-evidence-not-observed",
		PinsDigest: pinsDigest, IncidentID: authorization.IncidentID(), AuthorizationDigest: authorization.EnvelopeDigest(),
		ReceiptSetDigest: authorization.ReceiptSetDigest(), ReceiptCount: authorization.ReceiptCount(),
		SuccessorPlanDigest: authorization.SuccessorPlanDigest()}
	if action == "verify-authorization" {
		return report, nil
	}
	closureRoots, err := roots(pins.ClosureRoots)
	if err != nil {
		return VerificationReport{}, ErrInput
	}
	raw, err = readRegularFile(closurePath, maxVerificationRecordBytes, true)
	if err != nil {
		return VerificationReport{}, ErrInput
	}
	closure, err := recoverydisposal.VerifyClosure(raw, authorization, *pins.Closure, closureRoots)
	if err != nil {
		return VerificationReport{}, ErrInput
	}
	report.Stage = "closure-verified"
	report.ClosureDigest = closure.EnvelopeDigest
	report.Successor = &closure.Successor
	return report, nil
}
