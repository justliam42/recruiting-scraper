import requests
import re
import json
from collections import defaultdict
from bs4 import BeautifulSoup
import urllib3
import concurrent.futures
import csv

# Disable unverified HTTPS request warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

base_url = "https://www.regattacentral.com"
main_results_url = "https://www.regattacentral.com/regatta/results2?job_id=9168&org_id=0"


def get_event_links(html):
    soup = BeautifulSoup(html, "html.parser")
    event_links = []
    for a in soup.select('a[href^="/regatta/results2/eventResults.jsp"]'):
        href = a.get("href")
        if href.startswith("/"):
            href = base_url + href
        event_links.append(href)
    return event_links


def fetch_event_results_json(job_id, event_id):
    url = f"https://www.regattacentral.com/servlet/DisplayRacesResults?Method=getResults&job_id={job_id}&event_id={event_id}"
    try:
        resp = requests.get(url, verify=False, timeout=15)
        if resp.status_code == 200:
            return resp.text
    except Exception as e:
        print(f"Error fetching event results for job_id={job_id}, event_id={event_id}: {e}")
    return None


def fetch_lineup(job_id, boat_id):
    url = f"https://www.regattacentral.com/servlet/LineupServlet?Method=getLineupHtml&job_id={job_id}&boat_id={boat_id}"
    try:
        resp = requests.get(url, verify=False, timeout=10)
        if resp.status_code == 200:
            return parse_lineup_html(resp.text)
    except Exception as e:
        print(f"Error fetching lineup for job_id={job_id}, boat_id={boat_id}: {e}")
    return []


def parse_lineup_html(html):
    # Parse the LineupServlet HTML and return a list of athletes with seat, name, age, club.
    soup = BeautifulSoup(html, "html.parser")
    athletes = []
    for line in soup.get_text(separator="\n").splitlines():
        m = re.match(r"(\d+):\s*(.+?)\s*-\s*(\d+)\s*\((.+?)\)", line.strip())
        if m:
            seat, name, age, club = m.groups()
            athletes.append({
                "seat": seat,
                "name": name.strip(),
                "age": age,
                "club": club.strip(),
            })
    return athletes

def parse_event_results_json(json_str, job_id=None, event_name=None):
    data = json.loads(json_str)
    if not event_name:
        event_name = data.get("long_desc") or data.get("event_label") or ""
    results = []
    races = data.get("races", [])
    # Map boat_id to its lineup (athletes)
    boat_lineups = {}
    # Collect all (job_id, boat_id) pairs for parallel lineup fetching
    boat_ids = []
    race_info = []
    for race in races:
        race_name = race.get("stageName", "") or race.get("displayNumber", "") or race.get("raceName", "") or "Final"
        for result in race.get("results", []):
            boat_id = str(result.get("boatId")) if result.get("boatId") else None
            boat_label = result.get("boatLabel") or ""
            club_name = result.get("orgName") or result.get("longName") or ""
            place = (
                result.get("place")
                or result.get("orderOfFinishPlace")
                or result.get("finishPlace")
                or result.get("officialPlace")
                or ""
            )
            bow = result.get("lane", "")
            finish = (
                result.get("finishTimeString")
                or result.get("adjustedTimeString")
                or result.get("rawTimeString")
                or result.get("officialTimeString")
                or ""
            )
            margin = (
                result.get("marginString")
                or result.get("adjustedTimeDeltaString")
                or result.get("officialMarginString")
                or ""
            )
            race_info.append({
                "boat_id": boat_id,
                "boat_label": boat_label,
                "club_name": club_name,
                "place": place,
                "bow": bow,
                "finish": finish,
                "margin": margin,
                "race_name": race_name,
            })
            if boat_id and job_id:
                boat_ids.append((job_id, boat_id))
    # Fetch all lineups in parallel, but only once per (job_id, boat_id)
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        future_to_boat = {
            executor.submit(fetch_lineup, job_id, boat_id): (job_id, boat_id)
            for (job_id, boat_id) in set(boat_ids)
        }
        for future in concurrent.futures.as_completed(future_to_boat):
            job_id_b, boat_id_b = future_to_boat[future]
            try:
                boat_lineups[boat_id_b] = future.result()
            except Exception:
                boat_lineups[boat_id_b] = []
    # Build results
    for info in race_info:
        boat_id = info["boat_id"]
        club_name = info["club_name"]
        # Use the lineup if available, otherwise fallback to boat_label
        athletes = []
        if boat_id and boat_id in boat_lineups and boat_lineups[boat_id]:
            # Use the actual lineup, but only include athletes whose club matches the club for this result
            # (sometimes the lineup API returns athletes from other clubs if the boat_id is reused)
            for a in boat_lineups[boat_id]:
                # Only include if club matches, or if club is missing in the result, include all
                if club_name and a["club"] and club_name.lower() in a["club"].lower():
                    athletes.append(a)
                elif not club_name:
                    athletes.append(a)
            # If nothing matched, fallback to all
            if not athletes:
                athletes = boat_lineups[boat_id]
        else:
            # fallback: just use the boat label if no lineup
            if info["boat_label"]:
                athletes.append({
                    "name": info["boat_label"],
                    "seat": None,
                    "club": club_name,
                })
        if not athletes:
            continue
        results.append({
            "event": event_name,
            "race": info["race_name"],
            "place": info["place"],
            "bow": info["bow"],
            "club": club_name,
            "athletes": athletes,
            "finish": info["finish"],
            "margin": info["margin"],
        })
    return results


def aggregate_athletes(event_results):
    athletes = defaultdict(
        lambda: {
            "races": [],
            "clubs": set(),
            "seats": set(),
            "names": set(),
        }
    )
    for result in event_results:
        # Skip entries with missing finish or place == 999
        if not result.get("finish") or str(result.get("place")).strip() == "999":
            continue
        # For each athlete in the lineup, add a race entry for that athlete only once per race
        seen_names = set()
        for athlete in result["athletes"]:
            key = athlete["name"].strip()
            # Avoid duplicate race entries for the same athlete in the same race
            race_id = (
                result["event"],
                result["race"],
                result["place"],
                result["bow"],
                result["club"],
                result["finish"],
            )
            if (key, race_id) in seen_names:
                continue
            seen_names.add((key, race_id))
            athletes[key]["names"].add(athlete["name"])
            athletes[key]["clubs"].add(athlete.get("club", ""))
            if athlete.get("seat"):
                athletes[key]["seats"].add(athlete["seat"])
            athletes[key]["races"].append(
                {
                    "event": result["event"],
                    "race": result["race"],
                    "place": result["place"],
                    "bow": result["bow"],
                    "club": result["club"],
                    "finish": result["finish"],
                    "margin": result["margin"],
                }
            )
    return athletes


def write_athletes_to_csv(athletes, filename="athletes.csv"):
    # Flatten the athlete data for CSV output
    with open(filename, "w", newline='', encoding="utf-8") as csvfile:
        fieldnames = [
            "athlete_name", "club", "seat", "event", "race", "place", "bow", "finish", "margin"
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for name, info in athletes.items():
            for race in info["races"]:
                # Find the matching athlete dict for this race (by name and club)
                matching_athlete = None
                for a in info["names"]:
                    # Try to match club and name
                    if a == name:
                        matching_athlete = a
                        break
                # For each seat (if any), otherwise just write one row per race
                seats = info["seats"] if info["seats"] else [""]
                for seat in seats:
                    writer.writerow({
                        "athlete_name": name,
                        "club": ", ".join(info["clubs"]),
                        "seat": seat,
                        "event": race["event"],
                        "race": race["race"],
                        "place": race["place"],
                        "bow": race["bow"],
                        "finish": race["finish"],
                        "margin": race["margin"],
                    })


def main():
    with open("sample.txt", "r", encoding="utf-8") as f:
        html = f.read()
    event_links = get_event_links(html)

    # Parallelize fetching and parsing of event JSONs
    def fetch_and_parse(link):
        m = re.search(r'job_id=(\d+)&event_id=(\d+)', link)
        if not m:
            return []
        job_id, event_id = m.group(1), m.group(2)
        json_str = fetch_event_results_json(job_id, event_id)
        if not json_str:
            return []
        return parse_event_results_json(json_str, job_id=job_id)

    all_event_results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(fetch_and_parse, link) for link in event_links]
        for future in concurrent.futures.as_completed(futures):
            all_event_results.extend(future.result())

    print(all_event_results)
    athletes = aggregate_athletes(all_event_results)
    for name, info in athletes.items():
        print(f"Athlete: {name}")
        print(f"  Clubs: {', '.join(info['clubs'])}")
        print(f"  Seats: {', '.join(info['seats'])}")
        print(f"  Races:")
        for race in info["races"]:
            print(
                f"    - Event: {race['event']}, Race: {race['race']}, Place: {race['place']}, Club: {race['club']}, Finish: {race['finish']}"
            )
        print()
    write_athletes_to_csv(athletes)
    print("Wrote athlete data to athletes.csv")


if __name__ == "__main__":
    main()

