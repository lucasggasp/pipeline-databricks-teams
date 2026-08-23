import requests
import json

# Buscar resumo da tabela
df = spark.table("catalog_automation_teams_email.default.top_250_imdb_movies")

# Estatísticas gerais
total_filmes = df.count()
media_rating = df.select("averageRating").agg({"averageRating": "avg"}).collect()[0][0]
media_votos = df.select("numVotes").agg({"numVotes": "avg"}).collect()[0][0]

# Top 5 filmes por rating
top_filmes = df.select("primaryTitle", "averageRating", "startYear") \
    .orderBy("averageRating", ascending=False) \
    .limit(5) \
    .collect()

# Distribuição por década
from pyspark.sql.functions import floor, col, count as spark_count, avg as spark_avg, round as spark_round

decadas = df.filter(col("startYear").isNotNull()) \
    .groupBy((floor(col("startYear") / 10) * 10).alias("decada")) \
    .agg(
        spark_count("*").alias("quantidade"),
        spark_round(spark_avg("averageRating"), 2).alias("rating_medio")
    ) \
    .orderBy("decada", ascending=False) \
    .limit(5) \
    .collect()

# Montar payload com o resumo
payload = {
    "mensagem": "Resumo da Tabela IMDb Top 250",
    "resumo": {
        "total_filmes": total_filmes,
        "rating_medio_geral": round(media_rating, 2),
        "media_votos": int(media_votos),
        "top_5_filmes": [
            {
                "titulo": row["primaryTitle"],
                "rating": row["averageRating"],
                "ano": row["startYear"]
            }
            for row in top_filmes
        ],
        "distribuicao_decadas": [
            {
                "decada": int(row["decada"]),
                "quantidade": row["quantidade"],
                "rating_medio": row["rating_medio"]
            }
            for row in decadas
        ]
    }
}

# Exibir payload formatado
print("📊 Payload que será enviado:")
print(json.dumps(payload, indent=2, ensure_ascii=False))
print("\n" + "="*50 + "\n")

# URL do webhook do Power Automate
webhook_url = "SUA_URL_WEBHOOK"
# Headers
headers = {
    "Content-Type": "application/json"
}

try:
    # Fazendo a requisição POST para o webhook
    response = requests.post(webhook_url, json=payload, headers=headers)
    
    # Verificando o status da resposta
    if response.status_code == 200 or response.status_code == 202:
        print("✅ Webhook chamado com sucesso!")
        print(f"\nStatus Code: {response.status_code}")
        
        # Tentando imprimir a resposta
        if response.text:
            print("\nResposta do webhook:")
            try:
                response_json = response.json()
                print(json.dumps(response_json, indent=2, ensure_ascii=False))
            except:
                print(response.text)
        else:
            print("\nWebhook executado sem retorno de dados.")
    else:
        print(f"❌ Erro ao chamar webhook: {response.status_code}")
        print(f"Resposta: {response.text}")
        
except requests.exceptions.RequestException as e:
    print(f"❌ Erro na requisição: {e}")