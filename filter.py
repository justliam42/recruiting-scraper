import pandas as pd

EVENT_TYPE_WEIGHTS = {
    "8+": 3.0,
    "4+": 2.5,
    "4x": 2.0,
    "4-": 2.0,
    "4x+": 2.0,
}

AGE_GROUP_WEIGHTS = {
    "u19": 1.0,
    "u17": 0.7,
    "u16": 0.5,
}

def get_event_type(event_name):
    for t in EVENT_TYPE_WEIGHTS:
        if t in event_name.lower():
            return t
    return None

def get_age_group(event_name):
    name = event_name.lower()
    if "u16" in name:
        return "u16"
    if "u17" in name:
        return "u17"
    return "u19"

def prestige_score(event_name):
    event_type = get_event_type(event_name)
    age_group = get_age_group(event_name)
    type_weight = EVENT_TYPE_WEIGHTS.get(event_type, 0)
    age_weight = AGE_GROUP_WEIGHTS.get(age_group, 0)
    return type_weight * age_weight

def filter_and_score(df):
    # Filter
    df = df.copy()
    df["event_type"] = df["event"].apply(get_event_type)
    df["age_group"] = df["event"].apply(get_age_group)
    df = df[df["finish"].astype(str).str.strip() != ""]
    df = df[df["place"].astype(str).str.strip() != ""]

    # Group by event/race to count boats beaten
    df["numeric_place"] = df["place"].apply(lambda x: int(''.join(filter(str.isdigit, str(x)))) if any(c.isdigit() for c in str(x)) else 0)
    df = df[df["numeric_place"] > 0]
    df["multiplier"] = df["event"].apply(prestige_score)

    df["boats_beaten"] = (df["num_boats"] - df["numeric_place"]).clip(lower=0)

    df["prestige_score"] = df["boats_beaten"] * df["multiplier"]
    # Drop helper columns
    df = df.drop(columns=["event_type", "age_group", "numeric_place"])
    return df

if __name__ == "__main__":
    df = pd.read_csv("athletes.csv")
    df = df[df["age"] < 18]

    # Only keep womens events
    df = df[df["event"].str.lower().str.contains("women")]

    # Find all athlete names who have at least one event with a '+'
    coxed_athletes = set(df[df["event"].str.contains(r"\+", na=False)]["athlete_name"])
    # Keep only rows for those athletes
    df = df[df["athlete_name"].isin(coxed_athletes)]

    # Save the filtered DataFrame to a new CSV file
    df.to_csv("filtered-athletes.csv", index=False)

    df = filter_and_score(df)

    # make another csv that merges the athlete names with their prestige scores
    prestige_scores = df.groupby("athlete_name")["prestige_score"].sum().reset_index()
    prestige_scores = prestige_scores.sort_values(by="prestige_score", ascending=False)
    prestige_scores.to_csv("prestige-scores.csv", index=False)