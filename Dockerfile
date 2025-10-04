# Single-stage: build + run in the same image
FROM golang:1.25-alpine
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
ENV CGO_ENABLED=0
RUN go build -o ltv-monthly ./main.go

CMD ["./ltv-monthly"]
