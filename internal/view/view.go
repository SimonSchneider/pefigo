package view

import (
	"context"
	"net/http"
	"net/url"

	"github.com/SimonSchneider/pefigo/internal/model"
	"github.com/a-h/templ"
)

type View struct {
	ctx context.Context
	w   http.ResponseWriter
	r   *http.Request
}

func NewView(ctx context.Context, w http.ResponseWriter, r *http.Request) *View {
	return &View{ctx: ctx, w: w, r: r}
}

func (v *View) Render(c templ.Component) error {
	v.setupHeaders(false)
	ctx := context.WithValue(v.ctx, sidebarExpandedKey{}, isSidebarExpanded(v.r))
	return c.Render(ctx, v.w)
}

type sidebarExpandedKey struct{}

func isSidebarExpanded(r *http.Request) bool {
	c, err := r.Cookie("sidebar-expanded")
	if err != nil {
		return true // default: expanded
	}
	return c.Value != "false"
}

func getSidebarExpanded(ctx context.Context) bool {
	v, ok := ctx.Value(sidebarExpandedKey{}).(bool)
	if !ok {
		return true
	}
	return v
}

func (v *View) setupHeaders(cache bool) {
	v.w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if cache {
		v.w.Header().Set("Cache-Control", "public, max-age=3600")
	} else {
		v.w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		v.w.Header().Set("Pragma", "no-cache")
		v.w.Header().Set("Expires", "0")
	}
}

func nextEncoded(path string) string {
	return url.QueryEscape(path)
}

func billFaviconURL(bill model.Bill) string {
	domain := model.ExtractDomain(bill.URL)
	if domain == "" {
		return ""
	}
	return "/favicons/" + domain
}

func billCompanyName(bill model.Bill) string {
	return model.ExtractCompanyName(bill.URL)
}
