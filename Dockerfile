FROM node:20-slim AS builder

WORKDIR /app

# Install build dependencies
RUN npm install -g corepack && corepack enable

# Copy source and config
COPY package.json build.mjs tsconfig.json ./
COPY src/ ./src/

# Install dependencies and build
RUN npm install
RUN npm run build

# Production stage
FROM node:20-slim

WORKDIR /app

# Copy built output and runtime deps
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/package.json ./

ENV NODE_ENV=production
ENV PORT=7860

EXPOSE 7860

CMD ["node", "--enable-source-maps", "./dist/index.mjs"]
