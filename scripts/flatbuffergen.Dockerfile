FROM alpine:3.15
LABEL maintainer="The M3DB Authors <m3db@googlegroups.com>"
RUN apk add --update flatbuffers bash
ENTRYPOINT [ "flatc -h" ]

