FROM clojure:tools-deps-1.10.2.796-slim-buster AS builder

RUN apt-get update && apt-get install -y curl

RUN curl -sL https://deb.nodesource.com/setup_14.x | bash -
RUN apt-get update && apt-get install -y nodejs

RUN mkdir -p /usr/src/fluree-ledger
WORKDIR /usr/src/fluree-ledger

COPY deps.edn ./

RUN clojure -A:test -P

COPY package.json package-lock.json ./
RUN npm install

COPY . ./

RUN make uberjar
RUN make stage-release

FROM openjdk:11 AS runner

RUN mkdir -p /opt/fluree
COPY --from=builder /usr/src/fluree-ledger/build/* /opt/fluree/
WORKDIR /opt/fluree

# Create a user to own the fluree code
RUN groupadd fluree && useradd --no-log-init -g fluree -m fluree
# Copy deps from builder to fluree user's maven repo
COPY --from=builder /root/.m2 /home/fluree/.m2
RUN chown -R fluree.fluree /home/fluree/.m2

# Create runtime data volume
RUN mkdir -p /var/lib/fluree && chown fluree.fluree /var/lib/fluree
VOLUME /var/lib/fluree

# Take ownership of the WORKDIR
RUN chown -R fluree.fluree .
USER fluree

# Expose HTTP API
EXPOSE 8090

# Point runtime data paths at volume
ENV FLUREE_ARGS="-Dfdb-storage-file-root=/var/lib/fluree/"

ENTRYPOINT ["./fluree_start.sh"]
CMD []