package httpapi

import (
	"fmt"
	"net/http"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/buildcontract"
	apiv1 "github.com/cofy-x/kova/pkg/api/v1"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/labstack/echo/v4"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

func (s *Server) handleBuildResults(c echo.Context) error {
	build, err := s.getBuild(c.Request().Context(), c.Param("id"))
	if apierrors.IsNotFound(err) {
		return notFound(c)
	}
	if err != nil {
		return internalError(c, err)
	}
	if err := s.authorizeBuild(c.Request().Context(), principalFromContext(c), "get", build); err != nil {
		return forbidden(c)
	}
	outputs, err := apiOutputs(build.Status.Outputs)
	if err != nil {
		return internalContractError(c, err)
	}
	return c.JSON(http.StatusOK, apiv1.BuildResults{
		ID: build.Name, SourceURI: build.Spec.Source.URI, SourceDigest: build.Spec.Source.Digest,
		IdempotencyKey: build.Spec.IdempotencyKey, Outputs: outputs,
	})
}

func apiOutputs(outputs []kovav1.BuildOutput) ([]apiv1.BuildOutput, error) {
	out := make([]apiv1.BuildOutput, 0, len(outputs))
	for _, output := range outputs {
		immutableRef, err := immutableReference(output.Image, output.ManifestDigest)
		if err != nil {
			return nil, err
		}
		platform, err := buildcontract.NormalizePlatform(output.Platform)
		if err != nil {
			return nil, fmt.Errorf("normalize output platform: %w", err)
		}
		out = append(out, apiv1.BuildOutput{Format: output.Format, Image: output.Image, ManifestDigest: output.ManifestDigest, ImmutableRef: immutableRef, Platform: apiv1.Platform(platform)})
	}
	return out, nil
}

func immutableReference(image, manifestDigest string) (string, error) {
	normalized, err := buildcontract.NormalizeTarget(image)
	if err != nil {
		return "", fmt.Errorf("normalize output image reference: invalid tagged image")
	}
	tag, err := name.NewTag(normalized, name.StrictValidation)
	if err != nil {
		return "", fmt.Errorf("normalize output image reference: invalid tagged image")
	}
	digest, err := name.NewDigest(tag.Repository.Name()+"@"+manifestDigest, name.StrictValidation)
	if err != nil {
		return "", fmt.Errorf("normalize output image reference: invalid manifest digest")
	}
	return digest.Name(), nil
}
