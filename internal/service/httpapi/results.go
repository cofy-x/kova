package httpapi

import (
	"net/http"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"

	"github.com/labstack/echo/v4"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

func (s *Server) handleBuildResults(c echo.Context) error {
	build, err := s.getBuild(c.Request().Context(), c.Param("id"))
	if apierrors.IsNotFound(err) {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "job not found"})
	}
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	if err := s.authorizeBuild(c.Request().Context(), principalFromContext(c), "get", build); err != nil {
		return forbidden(c)
	}
	return c.JSON(http.StatusOK, buildResultsResponse{
		ID: build.Name, SourceURI: build.Spec.Source.URI, SourceDigest: build.Spec.Source.Digest,
		IdempotencyKey: build.Spec.IdempotencyKey, Outputs: apiOutputs(build.Status.Outputs),
	})
}

func apiOutputs(outputs []kovav1.BuildOutput) []BuildOutput {
	out := make([]BuildOutput, 0, len(outputs))
	for _, output := range outputs {
		out = append(out, BuildOutput{Format: output.Format, Image: output.Image, ManifestDigest: output.ManifestDigest})
	}
	return out
}
