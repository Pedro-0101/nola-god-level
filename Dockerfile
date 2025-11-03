FROM python:3.11-slim

WORKDIR /app

# Só o cliente psql (para rodar o .sql) — evita instalar toolchain desnecessária
RUN apt-get update && apt-get install -y \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copia e instala dependências Python
COPY requirements.txt .
RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código
COPY . .

# Comando padrão (substituído no docker-compose)
CMD ["python", "generate_data.py"]
