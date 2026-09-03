---
title: Arcforge API
temoji: ⚡
colorFrom: gray
colorTo: black
sdk: docker
app_port: 7860
pinned: false
---

# Arcforge Backend API

AI-powered vibe coding platform backend. Runs the God Mode pipeline with inter-agent negotiation, multi-file generation, and Supabase persistence.

## Required Secrets

| Secret | Description |
|--------|-------------|
| `PORT` | Server port (7860) |
| `SUPABASE_URL` | Supabase project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | Supabase service role JWT |
| `NVIDIA_API_KEY` | NVIDIA API key for LLM calls |
| `NVIDIA_API_URL` | NVIDIA API base URL |
| `NVIDIA_LEADER_MODEL` | Leader agent model (optional) |
| `NVIDIA_BACKEND_MODEL` | Backend agent model (optional) |
| `NVIDIA_FRONTEND_MODEL` | Frontend agent model (optional) |
| `LOG_LEVEL` | Log level (default: info) |