
---
Task ID: P-1 (backend)
Agent: Z.ai Code (main)
Task: Publish + public showcase APIs (SEO growth loop) — the backend side of the studio Publish button.

Work Log:
- src/routes/db.ts: POST /projects/:id/publish (stable slug minting, visibility=public, published_at stamp, 2MB previewHtml snapshot, description ≤280 chars, owner-scoped 404s); GET /showcase, GET /showcase/:slug, GET /showcase/:slug/preview — all PUBLIC (registered before requireAuth; masked creator emails on SEO surfaces).
- get-session/update-project responses now carry slug + published_at so the studio hydrates its LIVE state.
- Migration 006 applied live via Management API (slug, published_at, partial unique index, published_at DESC index).

Stage Summary:
- Commit c4a0df6 live on Render (arcforge-backend). Architecture unchanged: the db-ops edge function remains the only entry to these routes; the backend remains the only Supabase service-role holder.
