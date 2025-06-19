# Source:
# cat ibis/docker/postgres/Dockerfile @ ef79e64 and cc40bf5

FROM postgis/postgis:17-3.5-alpine AS pgvector-builder
RUN apk add --no-cache git build-base clang19 llvm19-dev llvm19 uv
RUN uvx pgxnclient install first_last_agg
RUN uvx pgxnclient install vector

FROM postgis/postgis:17-3.5-alpine
RUN apk add --no-cache postgresql17-plpython3
RUN ls -alF /usr/local/lib/postgresql/bitcode/
COPY --from=pgvector-builder /usr/local/lib/postgresql/bitcode/src/first_last_agg.index.bc /usr/local/lib/postgresql/bitcode/first_last_agg.index.bc
COPY --from=pgvector-builder /usr/local/lib/postgresql/bitcode/vector.index.bc /usr/local/lib/postgresql/bitcode/vector.index.bc
COPY --from=pgvector-builder /usr/local/lib/postgresql/vector.so /usr/local/lib/postgresql/vector.so
COPY --from=pgvector-builder /usr/local/share/postgresql/extension /usr/local/share/postgresql/extension
