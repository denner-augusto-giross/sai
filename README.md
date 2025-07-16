# SAI - Sistema de Assignação Inteligente

## 📖 Visão Geral

O **SAI (Sistema de Assignação Inteligente)** é uma automação em Python projetada para otimizar a atribuição de corridas de entrega. O sistema identifica corridas que estão "travadas" (sem um entregador atribuído por um período de tempo), encontra os melhores entregadores disponíveis com base numa série de regras de negócio e envia uma oferta de corrida via WhatsApp.

O sistema é composto por dois processos principais que funcionam em conjunto:
1.  **Offer Worker (`worker.py`):** Um processo contínuo e agendado que consulta o banco de dados periodicamente para encontrar corridas e provedores, aplica a lógica de correspondência e envia as ofertas.
2.  **Webhook Server (`webhook_server.py`):** Um servidor web que fica online 24/7 para receber as respostas dos entregadores (aceite/recusa) via webhooks do Chatguru e, em seguida, comanda a API interna para atribuir a corrida.

## 🏛️ Arquitetura

O fluxo de trabalho do sistema é dividido em duas fases:

1.  **Fase de Oferta (Worker):**
    * O `worker.py` é acionado por um agendador interno (`croniter`).
    * Ele executa a lógica em `main.py` para consultar o banco de dados AWS.
    * Filtra os provedores com base em regras (distância de 10km, bloqueios por loja).
    * Encontra a melhor correspondência (`order_id` + `provider_id`).
    * Usa o `chatguru_api.py` para atualizar campos personalizados e enviar um template de oferta via WhatsApp para o provedor.

2.  **Fase de Resposta (Webhook Server):**
    * O provedor responde à mensagem no WhatsApp.
    * O Chatguru aciona a ação "POST para URL" configurada no diálogo.
    * O `ngrok` (em desenvolvimento) ou o `CapRover` (em produção) encaminha a chamada para o nosso `webhook_server.py`.
    * O servidor analisa o payload JSON, verifica a resposta e os `campos_personalizados` (`order_id`, `provider_id`).
    * Se a resposta for positiva, ele usa o `internal_api.py` para se autenticar e atribuir a corrida na API interna da Giross.

## 🚀 Começando

Siga estas instruções para configurar o ambiente de desenvolvimento local.

### Pré-requisitos

* Python 3.11+
* Git

### Instalação

1.  **Clone o repositório:**
    ```bash
    git clone [https://seu-repositorio-aqui.git](https://seu-repositorio-aqui.git)
    cd nome-do-repo
    ```

2.  **Crie e ative um ambiente virtual:**
    ```bash
    # Para Windows
    python -m venv venv
    .\venv\Scripts\activate

    # Para macOS/Linux
    python3 -m venv venv
    source venv/bin/activate
    ```

3.  **Instale as dependências:**
    ```bash
    pip install -r requirements.txt
    ```

### Configuração

O projeto utiliza um ficheiro `.env` para gerir as credenciais e variáveis de ambiente.

1.  Crie uma cópia do ficheiro `.env.example` (se existir) ou crie um novo ficheiro chamado `.env`.
2.  Preencha as seguintes variáveis com as suas credenciais:

    ```env
    # Credenciais do Banco de Dados
    HOST_2="database-bi-giross.cptmx83n8hgj.us-east-2.rds.amazonaws.com"
    USER_2="seu_usuario_db"
    PASSWORD_2="sua_senha_db"
    PORT_2="3306"
    DATABASE_2="giross_producao"

    # Credenciais do Chatguru (WABA)
    CHAT_GURU_KEY="sua_chave_chatguru"
    CHAT_GURU_ACCOUNT_ID="seu_id_de_conta_chatguru"
    CHAT_GURU_PHONE_ID="seu_id_de_telefone_chatguru"
    CHAT_GURU_URL="[https://s14.chatguru.app/api/v1](https://s14.chatguru.app/api/v1)"

    # Credenciais para a API Interna da Giross
    GIROSS_EMAIL="seu_email_api_interna"
    GIROSS_PASSWORD="sua_senha_api_interna"

    # Token de Segurança do Webhook
    WEBHOOK_SECRET_TOKEN="sua_string_secreta_e_aleatoria_aqui"
    ```

## ▶️ Executando a Aplicação

Para executar o sistema localmente, você precisará de dois ou três terminais abertos.

### 1. Executando o Servidor Webhook

Este servidor recebe as respostas dos entregadores.

```bash
# Inicie o servidor com o Waitress para simular um ambiente de produção
waitress-serve --host 127.0.0.1 --port=5000 webhook_server:app
```

### 2. Expondo o Servidor com ngrok

Para que o Chatguru possa aceder ao seu servidor local.

```bash
# No diretório onde o ngrok está instalado
./ngrok http 5000
```
*Copie a URL "Forwarding" gerada e configure-a na ação "POST para URL" no seu diálogo do Chatguru.*

### 3. Executando o Worker de Ofertas

Este script irá buscar as corridas e enviar as ofertas.

```bash
# Executa a lógica de produção continuamente
python worker.py
```

## 🛠️ Teste e Depuração

O ficheiro `main.py` pode ser executado diretamente com argumentos de linha de comando para facilitar os testes.

* **Enviar uma única oferta para um número de teste:**
    ```bash
    python main.py --numero-teste 5511999999999 --limite 1
    ```

* **Inspecionar os dataframes no terminal (sem enviar ofertas):**
    ```bash
    python main.py --print-dfs
    ```

* **Investigar uma ordem específica:**
    ```bash
    python investigate_order.py ID_DA_ORDEM_AQUI
    ```

## 🚢 Deploy (CapRover)

O deploy é feito com duas aplicações separadas no CapRover, usando dois Dockerfiles:

1.  **`giross-server-ai`** (Servidor Web):
    * Faz o deploy usando o ficheiro `Dockerfile.web`.
    * Contagem de instâncias: `1`.
    * Mapeamento de porta: `5000` -> `80`.

2.  **`giross-sai-worker`** (Agendador):
    * Faz o deploy usando o ficheiro `Dockerfile.worker`.
    * Contagem de instâncias: `1`.

## 📁 Estrutura dos Ficheiros

* `main.py`: Contém a lógica principal de negócio para encontrar e corresponder corridas e provedores.
* `worker.py`: O agendador contínuo que executa a lógica do `main.py` em intervalos definidos.
* `webhook_server.py`: Servidor web Flask que recebe as respostas dos provedores.
* `query.py`: Centraliza todas as queries SQL usadas no projeto.
* `db.py`: Módulo para a conexão com o banco de dados.
* `chatguru_api.py`: Classe para interagir com a API do Chatguru (WABA).
* `internal_api.py`: Classe para interagir com a API interna da Giross.
* `Dockerfile.web`: Instruções de deploy para o servidor web.
* `Dockerfile.worker`: Instruções de deploy para o worker.