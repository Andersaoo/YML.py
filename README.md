GitLab Services Collector
📌 Что это?
Этот инструмент автоматически собирает информацию о Docker‑образах, используемых в ваших проектах на GitLab. Он проходит по всем репозиториям указанной группы, находит YAML‑файлы (кроме стандартных .gitlab-ci.yml и docker-compose.yml), извлекает оттуда названия сервисов и теги образов, а затем показывает структуру в удобном виде и сохраняет результаты в JSON, TXT или CSV.

Простыми словами: вы получаете единый каталог всех микросервисов и их версий, разбросанных по разным проектам.

🎯 Кому пригодится?
DevOps‑инженерам — чтобы видеть, какие образы где используются, и контролировать версии.

Разработчикам — для быстрого поиска, в каком проекте живёт нужный сервис.

Техлидам и архитекторам — для аудита и инвентаризации микросервисной архитектуры.

🚀 Как запустить?
Установите зависимости

bash
pip install -r requirements.txt
Настройте доступ к GitLab
Скопируйте файл .env.example в .env и отредактируйте его:

ini
GITLAB_PRIVATE_TOKEN=ваш_токен_с_правами_read_api_и_read_repository
GITLAB_GROUP=имя_вашей_группы   # например, project
Токен можно создать в GitLab: Settings → Access Tokens.

Запустите сбор

bash
python YML.py
Выберите формат сохранения
После завершения сбора скрипт предложит сохранить данные в JSON, TXT, CSV или сразу во всех форматах.

Результаты появятся в папке results/ (или той, что вы указали в .env).

📁 Пример вывода
text
project-name
——— backend
—————— auth-service: v2.1.0
—————— payment-api: 1.4.2

——— worker
—————— image-processor: latest
📦 Требования
Python 3.7+

Доступ к GitLab API (токен с правами read_api, read_repository)

Сделано с ❤️ для автоматизации рутины.

GitLab Services Collector
📌 What is it?
This tool automatically collects information about the Docker images used in your GitLab projects. It traverses all repositories in a specified group, finds YAML files (except for the standard .gitlab-ci.yml and docker-compose.yml), extracts service names and image tags, then displays the structure in a convenient format and saves the results in JSON, TXT, or CSV.

In simple terms, you get a single catalog of all microservices and their versions, scattered across different projects.

🎯 Who will find it useful??
DevOps engineers — to see which images are used where and to control versions.

Developers — to quickly find which project a given service lives in.

Tech leads and architects — to audit and inventory the microservice architecture.

🚀 How to run?
Install dependencies

bash
pip install -r requirements.txt
Configure GitLab access
Copy the .env.example file to .env and edit it:

ini
GITLAB_PRIVATE_TOKEN=your_token_with_read_api_and_read_repository_permissions
GITLAB_GROUP=your_group_name # e.g., project
You can create a token in GitLab: Settings → Access Tokens.

Run the collection

bash
python YML.py
Select the save format
After the collection is complete, the script will offer to save the data in JSON, TXT, CSV, or all formats.

The results will appear in the results/ folder (or the one you specified in .env).

📁 Sample output
text
project-name
——— backend
—————— auth-service: v2.1.0
—————— payment-api: 1.4.2

——— worker
—————— image-processor: latest
📦 Requirements
Python 3.7+

GitLab API access (token with read_api, read_repository permissions)

Made with ❤️ to automate routine tasks.