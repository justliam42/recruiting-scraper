import requests
import re
import json
from collections import defaultdict
from bs4 import BeautifulSoup
import urllib3
import concurrent.futures
import csv
import time

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

    # Collect all unique (job_id, boat_id) pairs for lineup fetching
    boat_ids = set()
    race_rows = []
    for race in races:
        race_name = (
            race.get("stageName", "")
            or race.get("displayNumber", "")
            or race.get("raceName", "")
            or "Final"
        )

        num_boats = 0
        rows = []
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
            if place != "": num_boats += 1

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
            rows.append({
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
                boat_ids.add((job_id, boat_id))

        for row in rows: row["num_boats"] = num_boats
        race_rows.extend(rows)

    # Fetch all lineups in parallel, once per (job_id, boat_id)
    import concurrent.futures
    boat_lineups = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        future_to_boat = {
            executor.submit(fetch_lineup, job_id, boat_id): (job_id, boat_id)
            for (job_id, boat_id) in boat_ids
        }
        for future in concurrent.futures.as_completed(future_to_boat):
            _, boat_id_b = future_to_boat[future]
            try:
                boat_lineups[boat_id_b] = future.result()
            except Exception:
                boat_lineups[boat_id_b] = []

    # Build results
    for info in race_rows:
        boat_id = info["boat_id"]
        club_name = info["club_name"]
        athletes = []
        if boat_id and boat_id in boat_lineups and boat_lineups[boat_id]:
            # Only include athletes whose club matches the club for this result
            for a in boat_lineups[boat_id]:
                if club_name and a["club"] and club_name.lower() in a["club"].lower():
                    athletes.append(a)
                elif not club_name:
                    athletes.append(a)
            if not athletes:
                athletes = boat_lineups[boat_id]
        elif info["boat_label"]:
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
            "num_boats": info["num_boats"],
        })
    return results


def aggregate_athletes(event_results):
    athletes = defaultdict(
        lambda: {
            "races": [],
            "clubs": set(),
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
            athletes[key]["age"] = athlete.get("age", "")
            athletes[key]["races"].append(
                {
                    "event": result["event"],
                    "race": result["race"],
                    "place": result["place"],
                    "bow": result["bow"],
                    "club": result["club"],
                    "finish": result["finish"],
                    "margin": result["margin"],
                    "seat": athlete.get("seat", ""),
                    "num_boats": result["num_boats"],
                }
            )
    return athletes


def write_athletes_to_csv(athletes, filename="athletes.csv"):
    # Flatten the athlete data for CSV output
    with open(filename, "w", newline='', encoding="utf-8") as csvfile:
        fieldnames = [
            "athlete_name", "age", "club", "seat", "event", "race", "place", "bow", "finish", "margin", "num_boats"
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for name, info in athletes.items():
            for race in info["races"]:
                writer.writerow({
                    "athlete_name": name,
                    "age": info.get("age", ""),
                    "club": ", ".join(info["clubs"]),
                    "seat": race["seat"],
                    "event": race["event"],
                    "race": race["race"],
                    "place": race["place"],
                    "bow": race["bow"],
                    "finish": race["finish"],
                    "margin": race["margin"],
                    "num_boats": race["num_boats"],
                })


def main():
    html = requests.get(main_results_url, verify=False).text
    event_links = get_event_links(html)

    # Sequential fetching and parsing of event JSONs (no concurrency)
    all_event_results = []
    for link in event_links:
        m = re.search(r'job_id=(\d+)&?.*&event_id=(\d+)', link)
        if not m:
            continue
        job_id, event_id = m.group(1), m.group(2)
        # Try up to 3 times to fetch event results if null
        json_str = None
        for attempt in range(3):
            json_str = fetch_event_results_json(job_id, event_id)
            if json_str:
                break
            print(f"Attempt {attempt+1} failed for job_id={job_id}, event_id={event_id}, retrying...")
            time.sleep(2)
        if not json_str:
            print(f"Skipping event {job_id=} {event_id=}: no data returned")
            continue
        time.sleep(1)
        event_results = parse_event_results_json(json_str, job_id=job_id)
        all_event_results.extend(event_results)

    athletes = aggregate_athletes(all_event_results)
    for name, info in athletes.items():
        print(f"Athlete: {name}")
        print(f"  Clubs: {', '.join(info['clubs'])}")
        print(f"  Races:")
        for race in info["races"]:
            print(
                f"    - Event: {race['event']}, Race: {race['race']}, Place: {race['place']}, Club: {race['club']}, Finish: {race['finish']}, Seat: {race['seat']}, Boat Count: {race['num_boats']}"
            )
        print()
    write_athletes_to_csv(athletes)
    print("Wrote athlete data to athletes.csv")


if __name__ == "__main__":
    main()

