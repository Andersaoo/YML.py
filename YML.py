import os
import json
import yaml
import requests
import time
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

import config


@dataclass
class GitLabConfig:
    """GitLab API configuration / Конфигурация GitLab API"""
    token: str
    url: str = "https://gitlab.com"
    group_path: str = ""
    max_projects: int = 500


class GitLabAPIClient:
    """GitLab API client with retries and rate limit handling / Клиент GitLab API с повторами и обработкой лимитов"""

    def __init__(self, config: GitLabConfig):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({"PRIVATE-TOKEN": config.token, "Content-Type": "application/json"})
        self.timeout = 30
        self.max_retries = 3

    def make_request(self, url: str, params: Dict = None, method: str = "GET") -> Optional[requests.Response]:
        """Execute request with exponential backoff on timeout/rate limit / Выполнить запрос с экспоненциальной задержкой"""
        for attempt in range(self.max_retries):
            try:
                if method == "GET":
                    response = self.session.get(url, params=params, timeout=self.timeout)
                else:  # POST
                    response = self.session.post(url, json=params, timeout=self.timeout)

                if response and response.status_code == 200:
                    return response
                elif response and response.status_code == 429:  # Rate limit
                    wait_time = 2 ** (attempt + 1)
                    print(f"⚠️  Rate limit, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"⚠️  HTTP {response.status_code} for {url}")
                    return None
            except requests.exceptions.Timeout:
                print(f"⚠️  Timeout attempt {attempt + 1}/{self.max_retries}")
                time.sleep(2 ** attempt)
            except Exception as e:
                print(f"❌ Request error: {e}")
                return None
        return None

    def test_connection(self) -> bool:
        """Verify GitLab connectivity / Проверить соединение с GitLab"""
        url = f"{self.config.url}/api/v4/version"
        response = self.make_request(url)
        if response:
            print(f"✅ Connected to GitLab {response.json().get('version')}")
            return True
        return False

    def get_group_id(self, group_path: str) -> Optional[int]:
        """Get group ID by full path / Получить ID группы по пути"""
        if not group_path:
            return None
        encoded = group_path.replace('/', '%2F')
        url = f"{self.config.url}/api/v4/groups/{encoded}"
        response = self.make_request(url)
        return response.json().get('id') if response else None

    def get_all_projects(self, group_id: Optional[int] = None) -> List[Dict]:
        """Fetch all projects (optionally under a group) / Получить все проекты (возможно, внутри группы)"""
        all_projects = []
        page = 1
        print("📋 Fetching project list...")
        while True:
            url = f"{self.config.url}/api/v4/groups/{group_id}/projects" if group_id else f"{self.config.url}/api/v4/projects"
            params = {"per_page": 50, "page": page, "simple": True, "order_by": "last_activity_at", "sort": "desc"}
            if group_id:
                params["include_subgroups"] = True

            response = self.make_request(url, params)
            if not response:
                break

            projects = response.json()
            if not projects:
                break

            all_projects.extend(projects)
            print(f"  📄 Loaded {len(all_projects)} projects")

            if self.config.max_projects and len(all_projects) >= self.config.max_projects:
                print(f"⚠️  Reached limit of {self.config.max_projects} projects")
                all_projects = all_projects[:self.config.max_projects]
                break

            if 'next' not in response.links:
                break
            page += 1
            time.sleep(0.1)  # be gentle to API

        return all_projects

    def get_project_files(self, project_id: int) -> List[Dict]:
        """Get recursive file tree of a project / Получить рекурсивное дерево файлов проекта"""
        url = f"{self.config.url}/api/v4/projects/{project_id}/repository/tree"
        params = {"recursive": True, "per_page": 100}
        response = self.make_request(url, params)
        return response.json() if response else []

    def get_file_content(self, project_id: int, file_path: str, ref: str = "main") -> Optional[str]:
        """Get raw content of a file / Получить содержимое файла"""
        encoded = file_path.replace('/', '%2F')
        url = f"{self.config.url}/api/v4/projects/{project_id}/repository/files/{encoded}/raw"
        response = self.make_request(url, {"ref": ref})
        return response.text if response else None


class YAMLAnalyzer:
    """YAML parsing and image tag extraction / Разбор YAML и извлечение тегов образов"""

    @staticmethod
    def extract_image_tag(image_string: str) -> str:
        """Extract tag from docker image string (handles variables) / Извлечь тег из строки образа (с переменными)"""
        if not image_string:
            return ""
        image_string = image_string.strip()
        if ':' in image_string:
            tag = image_string.split(':')[-1]
            # strip variable wrappers like ${...} or ${{...}}
            if tag.startswith('${') and tag.endswith('}'):
                tag = tag[2:-1]
            elif tag.startswith('${{') and tag.endswith('}}'):
                tag = tag[3:-2]
            return tag
        return image_string

    @staticmethod
    def find_services_in_yaml(content: str) -> Dict[str, str]:
        """Recursively find all 'image' fields in YAML / Рекурсивно найти все поля 'image' в YAML"""
        services = {}
        try:
            data = yaml.safe_load(content)
            if not data:
                return services

            def recursive_search(obj, path=""):
                if isinstance(obj, dict):
                    if 'image' in obj and isinstance(obj['image'], str):
                        tag = YAMLAnalyzer.extract_image_tag(obj['image'])
                        name = path or next((obj.get(n) for n in ['name', 'container_name', 'service'] if n in obj), "unnamed")
                        services[name] = tag
                    for k, v in obj.items():
                        if k not in ['image', 'build', 'networks', 'volumes', 'ports', 'environment']:
                            recursive_search(v, f"{path}.{k}" if path else k)
                elif isinstance(obj, list):
                    for i, item in enumerate(obj):
                        recursive_search(item, f"{path}[{i}]")

            recursive_search(data)
        except yaml.YAMLError:
            # fallback to regex for malformed YAML
            services = YAMLAnalyzer.extract_images_via_regex(content)
        except Exception as e:
            print(f"⚠️  YAML analysis error: {e}")
        return services

    @staticmethod
    def extract_images_via_regex(content: str) -> Dict[str, str]:
        """Fallback regex-based extraction / Запасной метод через регулярные выражения"""
        services = {}
        patterns = [
            r'^\s*image\s*:\s*["\']?([^"\'\n]+)["\']?',
            r'"image"\s*:\s*"([^"]+)"',
            r"'image'\s*:\s*'([^']+)'",
            r'^\s*(\w+)\s*:\s*\n\s+image\s*:\s*["\']?([^"\'\n]+)["\']?',
        ]
        for pattern in patterns:
            for match in re.finditer(pattern, content, re.MULTILINE | re.IGNORECASE):
                if len(match.groups()) == 1:
                    services[f"service_{len(services)}"] = YAMLAnalyzer.extract_image_tag(match.group(1))
                elif len(match.groups()) == 2:
                    services[match.group(1)] = YAMLAnalyzer.extract_image_tag(match.group(2))
        return services

    @staticmethod
    def normalize_service_name(name: str) -> str:
        """Clean up service name: remove brackets, extra underscores / Очистить имя сервиса"""
        if not name:
            return ""
        name = name.replace('-', '_').replace('.', '_')
        if name.startswith('services_'):
            name = name[9:]
        name = re.sub(r'\[.*?\]', '', name)
        name = re.sub(r'_+', '_', name)
        return name.strip('_')


class GitLabServiceCollector:
    """Main collector orchestrator / Основной сборщик"""

    def __init__(self, config: GitLabConfig):
        self.config = config
        self.api = GitLabAPIClient(config)
        self.analyzer = YAMLAnalyzer()
        self.stats = {"total_projects": 0, "projects_with_yaml": 0, "total_yaml_files": 0, "total_services": 0, "errors": 0}
        self.results = {}

    def analyze_project(self, project: Dict) -> Optional[Dict]:
        """Analyze a single project: fetch YAMLs, extract services / Анализировать один проект"""
        pid = project['id']
        name = project['name']
        print(f"  🔍 Analyzing project: {name}")

        files = self.api.get_project_files(pid)
        if not files:
            print("    ⚠️  No files or access error")
            return None

        yaml_files = []
        for f in files:
            if f.get('type') == 'blob' and f['name'].endswith(('.yml', '.yaml')):
                if f['name'] not in ['.gitlab-ci.yml', 'docker-compose.yml', 'docker-compose.yaml']:
                    yaml_files.append({'path': f['path'], 'name': f['name']})

        if not yaml_files:
            print("    ℹ️  No YAML files")
            return None

        print(f"    📄 YAML files: {len(yaml_files)}")
        project_services = {}

        for yf in yaml_files:
            content = self.api.get_file_content(pid, yf['path'])
            if not content:
                print(f"      ❌ Failed to get {yf['name']}")
                self.stats["errors"] += 1
                continue

            services = self.analyzer.find_services_in_yaml(content)
            if services:
                norm = {self.analyzer.normalize_service_name(s): t for s, t in services.items()}
                key = yf['name'].replace('.yml', '').replace('.yaml', '')
                if key.startswith('services_'):
                    key = key[9:]
                project_services[key] = norm
                self.stats["total_yaml_files"] += 1
                self.stats["total_services"] += len(services)
                print(f"      ✅ {yf['name']}: {len(services)} services")
            else:
                print(f"      ℹ️  {yf['name']}: no services found")

        if project_services:
            self.stats["projects_with_yaml"] += 1
            return {"project_id": pid, "project_name": name, "services": project_services}
        return None

    def collect_all_services(self, use_threads: bool = True, max_workers: int = 5):
        """Collect services from all rojects (optionally parallel) / СОбрать сервисы из всех проектов"""
        print("=" * 70)
        print("🚀 STARTING GITLAB DATA COLLECTION")
        print("=" * 70)

        if not self.api.test_connection():
            print("❌ Cannot connect to GitLab API")
            return

        group_id = None
        if self.config.group_path:
            group_id = self.api.get_group_id(self.config.group_path)
            if group_id:
                print(f"✅ Group found: {self.config.group_path} (ID: {group_id})")
            else:
                print(f"⚠️  Group not found: {self.config.group_path}")

        projects = self.api.get_all_projects(group_id)
        if not projects:
            print("❌ No projects retrieved")
            return

        self.stats["total_projects"] = len(projects)
        print(f"📊 Total projects to analyze: {len(projects)}")

        if use_threads and len(projects) > 1:
            print(f"⚡ Using threads ({max_workers} workers)")
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(self.analyze_project, p): p for p in projects}
                for f in as_completed(futures):
                    try:
                        res = f.result(timeout=60)
                        if res:
                            self.results[res["project_name"]] = res["services"]
                    except Exception as e:
                        print(f"❌ Error analyzing project {futures[f].get('name')}: {e}")
                        self.stats["errors"] += 1
        else:
            for p in projects:
                res = self.analyze_project(p)
                if res:
                    self.results[res["project_name"]] = res["services"]

        self.print_statistics()

    def print_statistics(self):
        """Print collection statistics / Вывести статистику"""
        print("\n" + "=" * 70)
        print("📊 STATISTICS")
        print("=" * 70)
        for k, v in self.stats.items():
            print(f"{k.replace('_',' ').title()}: {v}")
        if self.results:
            print(f"\n✅ Data collected from {len(self.results)} projects")
        else:
            print("\n❌ No data collected")

    def save_results(self, output_format: str = "all", output_dir: str = "."):
        """Save results to files (json/txt/csv) / Сохранить результаты в файлы"""
        if not self.results:
            print("❌ No data to save")
            return

        os.makedirs(output_dir, exist_ok=True)
        ts = time.strftime("%Y%m%d_%H%M%S")

        if output_format in ("json", "all"):
            data = {
                "metadata": {
                    "collected_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "gitlab_group": self.config.group_path or "all accessible projects",
                    "statistics": self.stats
                },
                "projects": self.results
            }
            path = os.path.join(output_dir, f"gitlab_services_{ts}.json")
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            print(f"💾 JSON saved: {path}")

        if output_format in ("text", "all"):
            path = os.path.join(output_dir, f"services_structure_{ts}.txt")
            with open(path, 'w', encoding='utf-8') as f:
                for proj, services in self.results.items():
                    f.write(f"{proj}\n")
                    for file_key, svc_dict in services.items():
                        f.write(f"——— {file_key}\n")
                        for svc, tag in svc_dict.items():
                            f.write(f"—————— {svc}: {tag}\n")
                        f.write("\n")
            print(f"📝 Text file saved: {path}")

        if output_format in ("csv", "all"):
            path = os.path.join(output_dir, f"services_{ts}.csv")
            with open(path, 'w', encoding='utf-8') as f:
                f.write("Project,File,Service,Tag\n")
                for proj, services in self.results.items():
                    for file_key, svc_dict in services.items():
                        for svc, tag in svc_dict.items():
                            f.write(f"{proj},{file_key},{svc},{tag}\n")
            print(f"📊 CSV saved: {path}")

    def print_structure(self):
        """Print hierarchical structure to console / ВЫвести иерархию в консоль"""
        if not self.results:
            print("❌ No data to display")
            return
        print("\n" + "=" * 70)
        print("🏗️  SERVICE STRUCTURE")
        print("=" * 70)
        for proj, services in self.results.items():
            print(proj)
            for file_key, svc_dict in services.items():
                print(f"——— {file_key}")
                for svc, tag in svc_dict.items():
                    print(f"—————— {svc}: {tag}")
                print()


def main():
    print("=" * 70)
    print("🚀 GITLAB SERVICES COLLECTOR")
    print("=" * 70)

    cfg = config.load_config()
    token = cfg.get("gitlab_token")
    if not token:
        print("❌ GITLAB_PRIVATE_TOKEN not set. Please provide it in .env file.")
        return

    gitlab_config = GitLabConfig(
        token=token,
        url=cfg.get("gitlab_url", "https://gitlab.com"),
        group_path=cfg.get("group_path", ""),
        max_projects=cfg.get("max_projects", 500)
    )

    collector = GitLabServiceCollector(gitlab_config)
    print("\n" + "=" * 70)
    print("⚡ COLLECTING DATA")
    print("=" * 70)

    collector.collect_all_services(use_threads=True, max_workers=5)

    if not collector.results:
        print("\n❌ No data collected.")
        return

    collector.print_structure()

    print("\n" + "=" * 70)
    print("💾 SAVE RESULTS")
    print("=" * 70)
    print("Choose format:\n  1. All (JSON, TXT, CSV)\n  2. Text only\n  3. JSON only\n  4. CSV only")
    choice = input("Your choice (1-4): ").strip()
    out_dir = cfg.get("output_dir", "results")
    formats = {"1": "all", "2": "text", "3": "json", "4": "csv"}
    collector.save_results(formats.get(choice, "all"), out_dir)
    print("\n✅ Done.")


if __name__ == "__main__":
    main()