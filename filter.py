import pandas as pd

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
    df.to_csv("filtered_athletes.csv", index=False)