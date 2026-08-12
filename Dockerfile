FROM node:22-alpine AS builder

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY tsconfig.json ./
COPY src ./src

RUN npm run build

# --- Production stage ---
FROM node:22-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci --omit=dev

COPY --from=builder /app/dist ./dist

# tsc only emits .ts -> .js, so the .sql migrations have to be copied in
# separately, next to the compiled migrate.js that resolves them.
COPY src/db/migrations ./dist/src/db/migrations

EXPOSE 8080

CMD ["node", "dist/src/index.js"]
