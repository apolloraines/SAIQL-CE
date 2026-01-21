#!/bin/bash
set -e

# SAIQL Production Deployment Script
# Automates deployment for Nova and other small-scale production environments

VERSION="5.0.0"
DEPLOY_TYPE="${1:-docker-compose}"
ENVIRONMENT="${2:-production}"

echo "🚀 SAIQL Production Deployment Script v$VERSION"
echo "================================================"
echo "Deploy Type: $DEPLOY_TYPE"
echo "Environment: $ENVIRONMENT"
echo ""

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    case $DEPLOY_TYPE in
        "docker-compose")
            if ! command -v docker &> /dev/null; then
                log_error "Docker is not installed"
                exit 1
            fi
            
            if ! command -v docker-compose &> /dev/null; then
                log_error "Docker Compose is not installed"
                exit 1
            fi
            ;;
        "kubernetes")
            if ! command -v kubectl &> /dev/null; then
                log_error "kubectl is not installed"
                exit 1
            fi
            
            # Check kubectl connection
            if ! kubectl cluster-info &> /dev/null; then
                log_error "Cannot connect to Kubernetes cluster"
                exit 1
            fi
            ;;
        "manual")
            if ! command -v python3 &> /dev/null; then
                log_error "Python 3 is not installed"
                exit 1
            fi
            ;;
        *)
            log_error "Unknown deployment type: $DEPLOY_TYPE"
            echo "Supported types: docker-compose, kubernetes, manual"
            exit 1
            ;;
    esac
    
    log_success "Prerequisites check passed"
}

# Function to create directories
setup_directories() {
    log_info "Setting up directories..."
    
    sudo mkdir -p /opt/saiql/{data,logs,backups,config}
    sudo chown -R $(whoami):$(whoami) /opt/saiql
    chmod 755 /opt/saiql/{data,logs,backups}
    chmod 750 /opt/saiql/config
    
    log_success "Directories created"
}

# Function to build Docker image
build_docker_image() {
    log_info "Building SAIQL Docker image..."
    
    cd "$(dirname "$0")/../.."
    
    docker build -f deployment/Dockerfile -t saiql/database:$VERSION .
    docker tag saiql/database:$VERSION saiql/database:latest
    
    log_success "Docker image built successfully"
}

# Function to deploy with Docker Compose
deploy_docker_compose() {
    log_info "Deploying SAIQL with Docker Compose..."
    
    cd "$(dirname "$0")/.."
    
    # Copy production config
    cp config/production.json /opt/saiql/config/
    
    # Start services
    docker-compose -f docker-compose.prod.yml down || true
    docker-compose -f docker-compose.prod.yml up -d
    
    # Wait for services to start
    log_info "Waiting for SAIQL to start..."
    sleep 30
    
    # Health check
    if curl -f http://localhost:5432/health &> /dev/null; then
        log_success "SAIQL is running and healthy"
    else
        log_error "SAIQL health check failed"
        docker-compose -f docker-compose.prod.yml logs saiql-database
        exit 1
    fi
    
    log_success "Docker Compose deployment completed"
}

# Function to deploy to Kubernetes
deploy_kubernetes() {
    log_info "Deploying SAIQL to Kubernetes..."
    
    cd "$(dirname "$0")/.."
    
    # Apply Kubernetes manifests
    kubectl apply -f kubernetes/saiql-deployment.yaml
    
    # Wait for deployment
    log_info "Waiting for Kubernetes deployment..."
    kubectl wait --for=condition=available --timeout=300s deployment/saiql-database
    
    # Check pod status
    kubectl get pods -l app=saiql
    
    # Get service info
    log_info "SAIQL service information:"
    kubectl get services -l app=saiql
    
    log_success "Kubernetes deployment completed"
}

# Function to deploy manually
deploy_manual() {
    log_info "Setting up manual deployment..."
    
    cd "$(dirname "$0")/../.."
    
    # Install Python dependencies
    pip3 install -r requirements.txt
    
    # Copy config
    cp deployment/config/production.json /opt/saiql/config/
    
    # Create systemd service
    sudo tee /etc/systemd/system/saiql.service > /dev/null <<EOF
[Unit]
Description=SAIQL Database Engine
After=network.target

[Service]
Type=simple
User=$(whoami)
WorkingDirectory=$(pwd)
Environment=SAIQL_ENV=production
Environment=PYTHONPATH=$(pwd)
ExecStart=/usr/bin/python3 saiql_production_server.py /opt/saiql/config/production.json
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF
    
    # Start and enable service
    sudo systemctl daemon-reload
    sudo systemctl enable saiql
    sudo systemctl start saiql
    
    # Check status
    sleep 10
    if sudo systemctl is-active --quiet saiql; then
        log_success "SAIQL service is running"
    else
        log_error "SAIQL service failed to start"
        sudo systemctl status saiql
        exit 1
    fi
    
    log_success "Manual deployment completed"
}

# Function to run post-deployment tests
run_tests() {
    log_info "Running post-deployment tests..."
    
    # Test basic connectivity
    if curl -f http://localhost:5432/health &> /dev/null; then
        log_success "Health check passed"
    else
        log_error "Health check failed"
        return 1
    fi
    
    # Test query execution
    if curl -f -X POST http://localhost:5432/query \
       -H "Content-Type: application/json" \
       -d '{"operation": "*", "parameters": [1, "test"]}' &> /dev/null; then
        log_success "Query execution test passed"
    else
        log_warning "Query execution test failed (may be expected)"
    fi
    
    # Test metrics endpoint
    if curl -f http://localhost:8080/metrics &> /dev/null; then
        log_success "Metrics endpoint test passed"
    else
        log_warning "Metrics endpoint test failed"
    fi
    
    log_success "Post-deployment tests completed"
}

# Function to show deployment info
show_deployment_info() {
    echo ""
    echo "🎉 SAIQL Deployment Information"
    echo "==============================="
    echo "Version: $VERSION"
    echo "Environment: $ENVIRONMENT"
    echo "Deploy Type: $DEPLOY_TYPE"
    echo ""
    echo "🔗 Endpoints:"
    echo "  Database: http://localhost:5432"
    echo "  Health: http://localhost:5432/health"
    echo "  Metrics: http://localhost:8080/metrics"
    echo "  API Docs: http://localhost:5432/docs"
    echo ""
    echo "📁 Data Directory: /opt/saiql/data"
    echo "📄 Logs Directory: /opt/saiql/logs"
    echo "💾 Backups Directory: /opt/saiql/backups"
    echo ""
    echo "🛠️ Management Commands:"
    case $DEPLOY_TYPE in
        "docker-compose")
            echo "  Stop: docker-compose -f deployment/docker-compose.prod.yml down"
            echo "  Logs: docker-compose -f deployment/docker-compose.prod.yml logs -f"
            echo "  Restart: docker-compose -f deployment/docker-compose.prod.yml restart"
            ;;
        "kubernetes")
            echo "  Status: kubectl get pods -l app=saiql"
            echo "  Logs: kubectl logs -l app=saiql -f"
            echo "  Scale: kubectl scale deployment saiql-database --replicas=N"
            ;;
        "manual")
            echo "  Status: sudo systemctl status saiql"
            echo "  Logs: sudo journalctl -u saiql -f"
            echo "  Restart: sudo systemctl restart saiql"
            ;;
    esac
    echo ""
    log_success "SAIQL is ready for production use!"
}

# Main deployment flow
main() {
    case "$DEPLOY_TYPE" in
        "docker-compose")
            check_prerequisites
            setup_directories
            build_docker_image
            deploy_docker_compose
            run_tests
            show_deployment_info
            ;;
        "kubernetes")
            check_prerequisites
            setup_directories
            build_docker_image
            deploy_kubernetes
            run_tests
            show_deployment_info
            ;;
        "manual")
            check_prerequisites
            setup_directories
            deploy_manual
            run_tests
            show_deployment_info
            ;;
        *)
            log_error "Invalid deployment type"
            exit 1
            ;;
    esac
}

# Handle script arguments
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    if [[ "$1" == "--help" || "$1" == "-h" ]]; then
        echo "SAIQL Production Deployment Script"
        echo ""
        echo "Usage: $0 [DEPLOY_TYPE] [ENVIRONMENT]"
        echo ""
        echo "DEPLOY_TYPE:"
        echo "  docker-compose  Deploy using Docker Compose (default)"
        echo "  kubernetes      Deploy to Kubernetes cluster"
        echo "  manual          Manual deployment with systemd"
        echo ""
        echo "ENVIRONMENT:"
        echo "  production      Production environment (default)"
        echo "  staging         Staging environment"
        echo ""
        echo "Examples:"
        echo "  $0 docker-compose production"
        echo "  $0 kubernetes staging"
        echo "  $0 manual production"
        exit 0
    fi
    
    main
fi
