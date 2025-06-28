#!/bin/bash

echo "🐳 Starting Supermarket Sales Data Warehouse System"
echo "=========================================================================="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running, please start Docker first"
    exit 1
fi

# Start PostgreSQL and pgAdmin
echo "🚀 Starting PostgreSQL database and pgAdmin..."
docker-compose up -d

# Wait for database to start
echo "⏳ Waiting for database to start..."
sleep 10

# Check database connection
echo "🔍 Checking database connection..."
docker exec datawarehouse_postgres pg_isready -U dwuser -d datawarehouse

if [ $? -eq 0 ]; then
    echo "✅ PostgreSQL database is running"
    echo ""
    echo "📋 Service Information:"
    echo "   PostgreSQL: localhost:5432"
    echo "   Database: datawarehouse"
    echo "   Username: dwuser"
    echo "   Password: dwpassword"
    echo ""
    echo "   pgAdmin: http://localhost:8080"
    echo "   Email: admin@datawarehouse.com"
    echo "   Password: admin123"
    echo ""
    echo "🔧 Installing Python dependencies..."
    pip3 install -r requirements.txt
    echo ""
    echo "✨ System startup complete! You can now run the data warehouse program:"
    echo "   python3 main.py"
else
    echo "❌ Database startup failed"
    docker-compose logs
fi 