# Simple Queue - Async Job Processing System

A real-time job queue processing system built with Go and React that demonstrates async task management with WebSocket notifications. Features job control (pause/resume/cancel), progress tracking, and clean architecture patterns.

## 🏗️ Architecture

This project implements **Hexagonal Architecture** (Ports & Adapters) with:

- **Core Domain**: Job entities and business logic
- **Ports**: Interfaces for external dependencies  
- **Adapters**: 
  - HTTP API (Fiber)
  - Message Queue (Asynq + Redis)
  - WebSocket Broadcasting
  - In-memory Repository
- **Frontend**: React + TypeScript dashboard with real-time updates

## ✨ Features

- ✅ **Async Job Processing** with Asynq + Redis
- ✅ **Real-time Updates** via WebSocket
- ✅ **Job Control**: Pause, Resume, Cancel operations
- ✅ **Progress Tracking** with live progress bars
- ✅ **Concurrent Workers** (configurable concurrency)
- ✅ **Clean Architecture** with dependency injection
- ✅ **Docker Support** with multi-stage builds

## 🚀 Quick Start

### Prerequisites
- Go 1.25+
- Node.js 20+
- Docker & Docker Compose
- Redis (via Docker)

### 1. Start Redis Queue
```bash
docker-compose -f compose.redis.yml up -d
```

### 2. Run the Full Stack
```bash
make run
```
This will:
- Start the Go API server on `:8080`
- Install frontend dependencies
- Start React dev server on `:5173`

### 3. Access the Application
- **Frontend Dashboard**: http://localhost:5173
- **API Endpoints**: http://localhost:8080
- **WebSocket**: ws://localhost:8080/ws/status

## 📚 API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/jobs` | List all jobs |
| `POST` | `/jobs` | Create new job |
| `POST` | `/jobs/:id/control` | Control job (PAUSE/RESTART/CANCEL) |
| `GET` | `/ws/status` | WebSocket for real-time updates |

## 🔧 Development Commands

### Build & Test
```bash
make all          # Build + test
make build        # Build binary only  
make test         # Run unit tests
make itest        # Run integration tests
```

### Development
```bash
make watch        # Live reload with Air
make run          # Start full stack
```

### Docker
```bash
make docker-run   # Start with Docker Compose
make docker-down  # Stop containers
```

### Cleanup
```bash
make clean        # Remove binaries
```

## 🏃‍♂️ How It Works

1. **Job Creation**: Frontend sends POST to `/jobs`
2. **Queue Enqueue**: Job gets queued in Redis via Asynq
3. **Worker Processing**: Asynq worker picks up and processes the job
4. **Real-time Updates**: Progress updates broadcast via WebSocket
5. **Job Control**: Users can pause/resume/cancel running jobs
6. **Status Tracking**: All state changes reflected in real-time UI

## 🧪 Testing

The project includes:
- **Unit Tests**: Core business logic
- **Integration Tests**: Database layer with Testcontainers
- **End-to-End**: Full workflow testing

```bash
# Run all tests
make test

# Integration tests with MySQL container
make itest
```

## 📦 Deployment

### Docker Production
```bash
docker-compose up --build
```

### Manual Deployment
1. Build: `make build` 
2. Build frontend: `cd frontend && npm run build`
3. Deploy binary + static files
4. Ensure Redis is available
5. Set environment variables

## 🛠️ Tech Stack

**Backend:**
- Go 1.25+ with Fiber framework
- Asynq for job queue management  
- Redis as message broker
- WebSocket for real-time communication
- Testcontainers for integration testing

**Frontend:**
- React 19 + TypeScript
- Tailwind CSS for styling
- Vite for development tooling
- WebSocket client for live updates

**Infrastructure:**
- Docker multi-stage builds
- Docker Compose orchestration
- Air for live reloading
- MySQL support (optional)

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.
