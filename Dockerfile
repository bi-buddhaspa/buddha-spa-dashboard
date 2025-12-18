FROM python:3.11-slim

# Não gerar .pyc e logar direto no terminal
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Pasta onde o código vai ficar
WORKDIR /app

# Instala as bibliotecas Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo o restante do código
COPY . .

# Configurações do Streamlit para rodar no Cloud Run
ENV PORT=8080
ENV STREAMLIT_SERVER_PORT=8080
ENV STREAMLIT_SERVER_HEADLESS=true
ENV STREAMLIT_SERVER_ENABLECORS=false
ENV STREAMLIT_SERVER_ENABLEXsSRFPROTECTION=false

EXPOSE 8080

# Comando que inicia o Streamlit
CMD ["streamlit", "run", "dashboard_windows.py", "--server.port=8080", "--server.address=0.0.0.0"]
