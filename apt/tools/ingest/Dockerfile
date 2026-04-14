# Stage 1: Build Phase
FROM node:20 AS builder
WORKDIR /app

# Install all dependencies (including dev) for compilation
COPY package*.json ./
RUN npm install

# Copy source and config
COPY tsconfig.json ./
COPY fetch-topic-urls.ts ./
COPY fetch-medium-article.ts ./

# Compile TypeScript to JavaScript
RUN npm run build

# Stage 2: Tiny Runtime Phase
FROM node:20-slim
WORKDIR /app

# Install CA certificates for TLS (scraping requirement)
RUN apt-get update && apt-get install -y ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Copy only production dependencies
COPY package*.json ./
RUN npm install --omit=dev

# Copy only the compiled output from the builder
COPY --from=builder /app/dist ./dist

# Standard entrypoint
ENTRYPOINT ["node"]
