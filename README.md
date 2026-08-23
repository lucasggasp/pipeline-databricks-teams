# Pipeline Automatizado: Databricks + Power Automate + Teams

Pipeline de automação de dados que extrai informações do Databricks, processa análises com PySpark e notifica automaticamente via Teams e Gmail através do Power Automate.

## 🎯 Visão Geral

Este projeto automatiza o processo de:
1. **Extração** de dados do Databricks usando Spark SQL
2. **Processamento** de análises e estatísticas
3. **Formatação** de dados em payload JSON estruturado
4. **Disparo** automático via webhook para Power Automate
5. **Notificação** no Teams e envio de email via Gmail

**Caso de uso:** Monitorar métricas de bases de dados críticas e manter stakeholders informados em tempo real.

## 🏗️ Arquitetura
┌─────────────────┐
│ Databricks │ Leitura de dados via Spark SQL
└────────┬────────┘
│
┌────────▼────────────────────┐
│ Script Python (PySpark) │ Cálculos e agregações
│ - Extração de métricas │
│ - Processamento de dados │
│ - Formatação JSON │
└────────┬────────────────────┘
│
┌────────▼────────────────────┐
│ Webhook HTTP POST │ Envio de payload estruturado
│ Power Automate │
└────────┬────────────────────┘
│
┌────┴────┐
│ │
┌───▼───┐ ┌──▼──┐
│ Teams │ │Gmail │ Notificações finais
└───────┘ └──────┘

## 📦 Pré-requisitos

- **Python 3.8+**
- **PySpark** (instalado no Databricks)
- **Databricks account** com acesso a tabelas
- **Power Automate** configurado com webhook
- **Biblioteca requests** para fazer chamadas HTTP

## 🚀 Instalação

### 1. Clonar o repositório

```bash
git clone https://github.com/lucasggasp/pipeline-databricks-teams.git
cd pipeline-databricks-teams
```

### 2. Instalar dependências

```bash
pip install -r requirements.txt
```

### 3. Configurar credenciais

Crie um arquivo `.env` na raiz do projeto:

```bash
WEBHOOK_URL=https://your-power-automate-webhook-url
```

**⚠️ Importante:** Nunca commite `.env` ou URLs de webhook no GitHub. O `.gitignore` já está configurado para ignorar esse arquivo.

### 4. Executar no Databricks

- Copie o script para um notebook do Databricks
- Substitua a variável `webhook_url` pela URL real
- Execute o notebook

## 💻 Uso

### Execução básica

```python
# No notebook do Databricks, execute:
exec(open("seu_script.py").read())
```

### Resultado esperado

Se tudo funcionar corretamente:
- ✅ Script exibe o payload que será enviado
- ✅ Webhook é disparado com sucesso (Status 200 ou 202)
- ✅ Notificação aparece no Teams
- ✅ Email é enviado via Gmail

## 📝 Estrutura do Código

### 1. Extração de Dados
```python
df = spark.table("catalog_automation_teams_email.default.top_250_imdb_movies")
total_filmes = df.count()
media_rating = df.select("averageRating").agg({"averageRating": "avg"}).collect()[0][0]
```
Lê a tabela Spark e calcula métricas agregadas.

### 2. Processamento de Dados
```python
top_filmes = df.select("primaryTitle", "averageRating", "startYear") \
    .orderBy("averageRating", ascending=False) \
    .limit(5) \
    .collect()
```
Filtra e ordena dados conforme necessário.

### 3. Formatação do Payload
```python
payload = {
    "mensagem": "Resumo da Tabela",
    "resumo": {
        "total_filmes": total_filmes,
        "rating_medio_geral": round(media_rating, 2),
        "top_5_filmes": [...]
    }
}
```
Estrutura JSON que será enviada ao Power Automate.

### 4. Envio via Webhook
```python
response = requests.post(webhook_url, json=payload, headers=headers)
if response.status_code in [200, 202]:
    print("✅ Webhook chamado com sucesso!")
```
Faz a chamada HTTP POST e valida a resposta.

## 🔄 Explicação do Fluxo

### Fluxo Completo

1. **Leitura (Databricks)**
   - Acessa tabela catalogada
   - Extrai ~250 registros de filmes

2. **Processamento (Python/PySpark)**
   - Calcula total de filmes
   - Média de ratings
   - Número médio de votos
   - Top 5 filmes melhor avaliados
   - Distribuição por década

3. **Formatação**
   - Monta estrutura JSON com todas as métricas
   - Valida tipos de dados
   - Garante precisão de casas decimais

4. **Envio (Webhook)**
   - POST request com headers Content-Type
   - Payload JSON estruturado
   - Resposta validada

5. **Power Automate**
   - Recebe webhook
   - Inicializa variáveis
   - Valida presença de anexos
   - Dispara notificações em paralelo

6. **Notificações Finais**
   - **Teams:** Mensagem formatada em canal
   - **Gmail:** Email com anexos (se houver)

## ⚠️ Tratamento de Erros

O script trata os seguintes cenários:

### Erro de Conexão Webhook
```python
except requests.exceptions.RequestException as e:
    print(f"❌ Erro na requisição: {e}")
```

### Erro de Status HTTP
```python
if response.status_code not in [200, 202]:
    print(f"❌ Erro ao chamar webhook: {response.status_code}")
```

### Erro de Parsing JSON
```python
try:
    response_json = response.json()
except:
    print(response.text)
```

## 🔍 Monitoramento e Logs

Para debug mais detalhado, adicione logs:

```python
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info(f"Total de filmes processados: {total_filmes}")
logger.info(f"Payload enviado: {json.dumps(payload)}")
logger.info(f"Status da resposta: {response.status_code}")
```

## 🚀 Melhorias Futuras

- [ ] **Agendamento automático** com Databricks Jobs
- [ ] **Retry logic** com backoff exponencial para falhas
- [ ] **Logging estruturado** em arquivo ou Cloud Storage
- [ ] **Tratamento de exceções** mais granular
- [ ] **Teste unitários** para funções críticas
- [ ] **Parametrização** de tabelas e métricas
- [ ] **Notificações condicionales** (alertas apenas se métrica ultrapassar threshold)
- [ ] **Histórico de execuções** em tabela Spark
- [ ] **Dashboard** para monitorar execuções

## 🔐 Segurança

- ✅ `.gitignore` protege credenciais
- ✅ URLs de webhook não são versionadas
- ✅ Variáveis de ambiente para dados sensíveis
- ⚠️ Revisar permissões de acesso ao Databricks
- ⚠️ Limitar permissões do Power Automate ao mínimo necessário

## 📚 Tecnologias Utilizadas

| Tecnologia | Versão | Propósito |
|------------|--------|----------|
| Python | 3.8+ | Linguagem principal |
| PySpark | Databricks | Processamento distribuído |
| requests | 2.28+ | Chamadas HTTP |
| Power Automate | Cloud | Orquestração e notificações |
| Teams | Cloud | Comunicação |
| Gmail | API | Email |

## 👤 Autor

Lucas Gomes  
Santander Brasil - Data Factory Intern
[GitHub](https://github.com/lucasggasp)

---

**Última atualização:** Agosto 2026  
**Licença:** MIT
