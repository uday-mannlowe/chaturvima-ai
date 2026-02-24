# 🚀 ChaturVima EC2 Deployment Guide (Docker)

## 📋 Prerequisites
- AWS Account with EC2 access
- Your project code ready
- GROQ_API_KEY available

---

## STEP 1 — Launch EC2 Instance

1. Go to **AWS Console → EC2 → Launch Instance**
2. Choose:
   - **AMI:** Ubuntu Server 22.04 LTS (Free Tier eligible)
   - **Instance type:** `t3.small` or `t3.medium` (recommended for AI workloads)
   - **Key pair:** Create or use existing `.pem` key
3. **Security Group — open these ports:**

   | Port | Protocol | Source        | Purpose          |
   |------|----------|---------------|------------------|
   | 22   | TCP      | Your IP       | SSH access       |
   | 5000 | TCP      | 0.0.0.0/0     | FastAPI app      |
   | 80   | TCP      | 0.0.0.0/0     | HTTP (optional)  |

4. **Storage:** 20 GB (gp3) minimum
5. Click **Launch Instance**

---

## STEP 2 — Connect to EC2

```bash
# On your local machine
chmod 400 your-key.pem

ssh -i your-key.pem ubuntu@<EC2_PUBLIC_IP>
```

---

## STEP 3 — Install Docker on EC2

```bash
# Update packages
sudo apt update && sudo apt upgrade -y

# Install Docker
sudo apt install -y docker.io docker-compose-plugin

# Start Docker and enable on boot
sudo systemctl start docker
sudo systemctl enable docker

# Add ubuntu user to docker group (no sudo needed)
sudo usermod -aG docker ubuntu

# IMPORTANT: Re-login to apply group changes
exit
ssh -i your-key.pem ubuntu@<EC2_PUBLIC_IP>

# Verify Docker works
docker --version
docker compose version
```

---

## STEP 4 — Upload Project to EC2

### Option A — Using SCP (from your Windows machine)
```bash
# Open terminal on your LOCAL machine (not EC2)
scp -i your-key.pem -r "C:/uday mannlowe/chaturvima/implementation" ubuntu@<EC2_PUBLIC_IP>:/home/ubuntu/chaturvima
```

### Option B — Using Git (recommended)
```bash
# On EC2, clone from GitHub
git clone https://github.com/yourusername/chaturvima.git
cd chaturvima
```

### Option C — Using SFTP (WinSCP / FileZilla)
- Host: `<EC2_PUBLIC_IP>`
- Username: `ubuntu`
- Key file: your `.pem` file
- Upload the entire `implementation` folder

---

## STEP 5 — Set Up Environment Variables on EC2

```bash
# Navigate to project directory
cd /home/ubuntu/chaturvima

# Create .env file with your secrets
cat > .env << 'EOF'
GROQ_API_KEY=your_groq_api_key_here
MAX_CONCURRENT_GENERATIONS=5
MAX_QUEUE_SIZE=100
GROQ_RATE_LIMIT_PER_MINUTE=30
GROQ_TIMEOUT_SECONDS=120
EOF

# Secure the .env file
chmod 600 .env
```

---

## STEP 6 — Build and Run with Docker Compose

```bash
# Navigate to project
cd /home/ubuntu/chaturvima

# Build the Docker image
docker compose build

# Start the container (detached mode = runs in background)
docker compose up -d

# Verify it's running
docker compose ps

# Check logs
docker compose logs -f
```

---

## STEP 7 — Test the Deployment

```bash
# From EC2 (localhost test)
curl http://localhost:5000/health

# From your browser or Postman
# http://<EC2_PUBLIC_IP>:5000/health
# http://<EC2_PUBLIC_IP>:5000/metrics
# http://<EC2_PUBLIC_IP>:5000/employee-report/<employee_id>
```

Expected response:
```json
{
  "status": "healthy",
  "workers": 5,
  "queue": { ... }
}
```

---

## STEP 8 — Useful Docker Commands

```bash
# View running containers
docker ps

# View logs (live)
docker compose logs -f chaturvima

# Restart container
docker compose restart

# Stop container
docker compose down

# Rebuild after code changes
docker compose down
docker compose build --no-cache
docker compose up -d

# Enter container shell (for debugging)
docker exec -it chaturvima-api bash

# Check container resource usage
docker stats
```

---

## STEP 9 — Auto-Restart on EC2 Reboot

Docker Compose already has `restart: unless-stopped`, but to start Docker on reboot:

```bash
sudo systemctl enable docker

# Optional: create a systemd service for docker-compose
sudo nano /etc/systemd/system/chaturvima.service
```

Paste this:
```ini
[Unit]
Description=ChaturVima Docker Compose App
After=docker.service
Requires=docker.service

[Service]
WorkingDirectory=/home/ubuntu/chaturvima
ExecStart=/usr/bin/docker compose up -d
ExecStop=/usr/bin/docker compose down
Restart=always
User=ubuntu

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable chaturvima
sudo systemctl start chaturvima
```

---

## 🔒 Security Best Practices

1. **Never commit `.env` to Git** — add it to `.gitignore`
2. **Use AWS Secrets Manager** for production API keys
3. **Restrict port 5000** to specific IPs if not public-facing
4. **Use Nginx as reverse proxy** on port 80/443 with SSL for production

---

## 🌐 Optional: Nginx Reverse Proxy (Port 80)

```bash
sudo apt install -y nginx

sudo nano /etc/nginx/sites-available/chaturvima
```

Paste:
```nginx
server {
    listen 80;
    server_name <EC2_PUBLIC_IP>;

    location / {
        proxy_pass http://localhost:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_read_timeout 300s;
        proxy_connect_timeout 10s;
    }
}
```

```bash
sudo ln -s /etc/nginx/sites-available/chaturvima /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl restart nginx
```

Now your API is accessible at: `http://<EC2_PUBLIC_IP>/health`

---

## 📌 Quick Reference

| Item              | Value                            |
|-------------------|----------------------------------|
| App Port          | 5000                             |
| Health Check URL  | `http://<IP>:5000/health`        |
| Employee Report   | `GET /employee-report/{emp_id}`  |
| JSON Report       | `GET /employee-report/{id}/json` |
| Metrics           | `GET /metrics`                   |
| Submit Job        | `POST /submit`                   |
| Job Status        | `GET /status/{job_id}`           |
