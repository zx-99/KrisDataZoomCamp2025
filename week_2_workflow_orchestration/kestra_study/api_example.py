import requests

r= requests.get("httpss://api.github.com/repos/kestra.io.kestra")
gh_stars = r.json()['star_count']

print(gh_stars)