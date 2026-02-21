# Paracel Opinion Monitor

Repositorio para monitoreo diario de menciones y tono (proxy de sentimiento) en medios y agregadores sobre **PARACEL**.
Genera un dataset (CSV/Parquet/JSON) y publica un tablero interactivo como **GitHub Pages** (sitio estático).

## Pipeline
1. Recolecta menciones desde:
   - GDELT 2.1 DOC API (noticias globales)
   - Google News RSS (agregador)
   - RSS adicionales (configurables)
2. Extrae texto del artículo (trafilatura) y normaliza metadatos.
3. Calcula:
   - Sentimiento proxy (reglas léxicas configurable, sin modelos pesados)
   - Etiquetas temáticas (taxonomía configurable por regex)
4. Construye artefactos:
   - `data/paracel_mentions.parquet`
   - `data/paracel_mentions.csv`
   - `docs/data/latest.json` (para el tablero)
   - `docs/index.html` (tablero interactivo)

## Requisitos
- Python 3.11+
- Dependencias en `requirements.txt`

## Ejecución local
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt

python scripts/run_daily.py --days-back 180
python scripts/build_site.py
```

## Publicación en GitHub Pages
1. Suba este repositorio a GitHub.
2. En GitHub: Settings -> Pages
   - Source: Deploy from a branch
   - Branch: main
   - Folder: /docs
3. El tablero quedará disponible en la URL de Pages del repo.

## Automatización diaria
El workflow `.github/workflows/daily.yml` ejecuta diariamente el pipeline, genera `docs/` y hace commit automático.

## Configuración
Edite `config/config.yml` para ajustar:
- queries
- RSS adicionales
- taxonomía por tópicos
- diccionario de sentimiento proxy
