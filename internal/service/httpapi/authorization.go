package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	serviceauth "github.com/cofy-x/kova/internal/service/auth"

	"github.com/labstack/echo/v4"
)

const (
	principalContextKey = "kova.principal"
	requesterLabel      = "kova.cofy.dev/requester-id"
)

func (s *Server) authMiddleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		token, err := serviceauth.Bearer(c.Request().Header.Get("Authorization"))
		if err != nil && s.cfg.AuthMode != serviceauth.ModeUnsafeNone {
			authDenied.Add(c.Request().Context(), 1)
			return c.JSON(http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		}
		if s.auth == nil {
			authDenied.Add(c.Request().Context(), 1)
			return c.JSON(http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		}
		principal, err := s.auth.Authenticate(c.Request().Context(), token)
		if err != nil {
			authDenied.Add(c.Request().Context(), 1)
			return c.JSON(http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		}
		c.Set(principalContextKey, principal)
		return next(c)
	}
}

func principalFromContext(c echo.Context) serviceauth.Principal {
	principal, _ := c.Get(principalContextKey).(serviceauth.Principal)
	return principal
}

func requesterID(username string) string {
	sum := sha256.Sum256([]byte(username))
	return hex.EncodeToString(sum[:16])
}

func (s *Server) authorize(ctx context.Context, principal serviceauth.Principal, verb, name string) error {
	if s.authz == nil {
		return fmt.Errorf("authorization is not configured")
	}
	return s.authz.Authorize(ctx, principal, serviceauth.Attributes{
		Verb: verb, Namespace: s.cfg.Namespace, Resource: serviceauth.ServiceBuildResource, Name: name,
	})
}

func (s *Server) authorizeBuild(ctx context.Context, principal serviceauth.Principal, verb string, build *kovav1.KovaBuild) error {
	if err := s.authorize(ctx, principal, verb, build.Name); err == nil {
		return nil
	}
	if principal.Username != "" && build.Spec.Requester.Username == principal.Username {
		return nil
	}
	return fmt.Errorf("access denied")
}

func forbidden(c echo.Context) error {
	authzDenied.Add(c.Request().Context(), 1)
	return c.JSON(http.StatusForbidden, map[string]string{"error": "forbidden"})
}
