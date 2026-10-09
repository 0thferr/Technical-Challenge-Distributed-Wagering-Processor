FROM oven/bun:1.3.3
WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile
COPY . .
RUN bun run build
EXPOSE 3000
ENTRYPOINT ["sh", "/app/docker/app-entrypoint.sh"]
CMD ["bun", "run", "start"]
