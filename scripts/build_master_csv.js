// scripts/build_master_csv.js
import fs from "fs";
import path from "path";
import csv from "csv-parser";

import { intangibleScores } from "../data/ai_intangible_scores.js";

const aiScoreMap = {};
intangibleScores.forEach(({ city, iso2, aiScore }) => {
  // Reuse your existing "normalizeKey" approach
  const key = normalizeKey(city, iso2);
  aiScoreMap[key] = aiScore;
});

// Utility function: remove spaces and punctuation, lowercase, etc.
function normalizeKey(cityName, iso2) {
  if (!cityName || !iso2) return "";
  const cleanCity = cityName.toLowerCase().replace(/[^a-z0-9]/g, "");
  const cleanCountry = iso2.toLowerCase().replace(/[^a-z0-9]/g, "");
  return `${cleanCity}${cleanCountry}`;
}

// 1) File paths to raw data
const paths = {
  core25: path.resolve("data/core25.csv"),
  simplemaps: path.resolve("data/raw_data/simplemaps-worldcities.csv"),
  pico: path.resolve("data/raw_data/pico/worldcities.csv"),
  traffic: path.resolve("data/2023-airport-traffic.csv"),
  output: path.resolve("data/2024-city-master.csv"), // final output
};

// 2) Parse a CSV file into an array of rows
async function parseCSV(filePath) {
  return new Promise((resolve, reject) => {
    const rows = [];
    fs.createReadStream(filePath)
      .pipe(csv())
      .on("data", (row) => rows.push(row))
      .on("end", () => resolve(rows))
      .on("error", (err) => reject(err));
  });
}

// 3) Build a population map from the big CSV(s)
async function buildPopulationMap() {
  const populationMap = {};

  // Helper to store or update populationMap
  function storePopulation(row) {
    const city = row.city?.trim();
    const iso2 = row.iso2?.trim();
    const pop = parseInt(row.population, 10) || 0;
    if (!city || !iso2 || !pop) return;

    const key = normalizeKey(city, iso2);
    // If we already have an entry for key, take the max population
    if (!populationMap[key] || pop > populationMap[key].population) {
      populationMap[key] = {
        city,
        country: row.country?.trim() || "",
        iso2: iso2.toLowerCase(),
        population: pop,
      };
    }
  }

  // Parse simplemaps-worldcities.csv
  const simpleRows = await parseCSV(paths.simplemaps);
  simpleRows.forEach((row) => storePopulation(row));

  // Parse pico/worldcities.csv (optional if you want to supplement)
  const picoRows = await parseCSV(paths.pico);
  picoRows.forEach((row) => storePopulation(row));

  return populationMap;
}

// 4) Build a traffic map from 2023-airport-traffic.csv
async function buildTrafficMap() {
  const trafficMap = {};
  const trafficRows = await parseCSV(paths.traffic);

  trafficRows.forEach((row) => {
    // City/Airport might be "New York City" or "San Francisco Bay Area"
    // We'll guess a city name from the row. For smaller data, do a manual approach.
    let cityName = row["City/Airport"]?.split("/")[0].trim();
    if (!cityName) return;

    const totalPax =
      parseInt(row["Total Passengers 2023"]?.replace(/,/g, ""), 10) || 0;
    const intPax =
      parseInt(row["International Passengers 2023"]?.replace(/,/g, ""), 10) ||
      0;
    const cargo =
      parseInt(row["Cargo Traffic 2023 (tons)"]?.replace(/,/g, ""), 10) || 0;

    // If we want to unify cityName with iso2, we might do special handling:
    // e.g., if cityName = "New York City", iso2 = "us". You can do a small lookup or direct if-else here:
    // For now, let's just pick a known iso2 for the main "City/Airport" in the CSV:
    // We'll do a simplistic approach:
    let isoGuess = "";
    if (cityName.toLowerCase().includes("london")) isoGuess = "gb";
    else if (cityName.toLowerCase().includes("new york")) isoGuess = "us";
    else if (cityName.toLowerCase().includes("tokyo")) isoGuess = "jp";
    else if (cityName.toLowerCase().includes("paris")) isoGuess = "fr";
    else if (cityName.toLowerCase().includes("dubai")) isoGuess = "ae";
    else if (cityName.toLowerCase().includes("hong kong")) isoGuess = "hk";
    else if (cityName.toLowerCase().includes("istanbul")) isoGuess = "tr";
    else if (cityName.toLowerCase().includes("singapore")) isoGuess = "sg";
    else if (cityName.toLowerCase().includes("madrid")) isoGuess = "es";
    else if (cityName.toLowerCase().includes("rome")) isoGuess = "it";
    else if (cityName.toLowerCase().includes("frankfurt")) isoGuess = "de";
    // ...
    // Expand if needed. Otherwise, many won't get matched. Or skip iso for lesser-known airports.

    if (!isoGuess) {
      // optional: console.log(`No iso guess for city: ${cityName}`);
      return;
    }

    const key = normalizeKey(cityName, isoGuess);

    trafficMap[key] = {
      totalPassengers: totalPax,
      internationalPassengers: intPax,
      cargoTraffic: cargo,
    };
  });

  return trafficMap;
}

// 5) Parse the core25 CSV
async function loadCore25() {
  const core25rows = await parseCSV(paths.core25);
  // We'll store them in an array
  return core25rows.map((row) => {
    return {
      city: row.city.trim(),
      country: row.country.trim(),
      iso2: row.iso2.toLowerCase(),
      population: parseInt(row.population, 10) || 0,
      totalPassengers: parseInt(row.totalPassengers, 10) || 0,
      internationalPassengers: parseInt(row.internationalPassengers, 10) || 0,
      cargoTraffic: parseInt(row.cargoTraffic, 10) || 0,
    };
  });
}

// 6) Main function to orchestrate everything
(async () => {
  try {
    console.log("Building population map...");
    const populationMap = await buildPopulationMap();

    console.log("Building traffic map...");
    const trafficMap = await buildTrafficMap();

    console.log("Loading core 25...");
    const core25 = await loadCore25();
    const core25Set = new Set(core25.map((c) => normalizeKey(c.city, c.iso2)));

    console.log("Combining all city keys...");
    const allKeys = new Set([
      ...Object.keys(populationMap),
      ...Object.keys(trafficMap),
    ]);

    const candidates = [];
    allKeys.forEach((key) => {
      const popEntry = populationMap[key] || {};
      const trafficEntry = trafficMap[key] || {
        totalPassengers: 0,
        internationalPassengers: 0,
        cargoTraffic: 0,
      };

      const intangible = aiScoreMap[key] || 0;

      if (!popEntry.city || !popEntry.country) return; // skip incomplete

      const finalScore =
        popEntry.population * 0.3 +
        trafficEntry.totalPassengers * 0.2 +
        trafficEntry.internationalPassengers * 0.2 +
        trafficEntry.cargoTraffic * 10 +
        intangible * 2_000_000;

      candidates.push({
        city: popEntry.city,
        country: popEntry.country,
        iso2: popEntry.iso2,
        population: popEntry.population,
        totalPassengers: trafficEntry.totalPassengers,
        internationalPassengers: trafficEntry.internationalPassengers,
        cargoTraffic: trafficEntry.cargoTraffic,
        intangible,
        finalScore,
      });
    });

    // Exclude the ones already in core25
    const filteredCandidates = candidates.filter(
      (c) => !core25Set.has(normalizeKey(c.city, c.iso2))
    );

    filteredCandidates.sort((a, b) => b.finalScore - a.finalScore);

    // Get top 75
    const top75 = filteredCandidates.slice(0, 75);

    // Merge with core25
    const final100 = [...core25, ...top75];

    // Output CSV
    const header =
      "city,country,iso2,population,totalPassengers,internationalPassengers,cargoTraffic,intangible,finalScore";
    const lines = final100.map((row) => {
      return [
        row.city,
        row.country,
        row.iso2.toUpperCase(),
        row.population,
        row.totalPassengers,
        row.internationalPassengers,
        row.cargoTraffic,
        row.intangible,
        row.finalScore,
      ].join(",");
    });

    fs.writeFileSync(paths.output, [header, ...lines].join("\n"), "utf8");
    console.log(`DONE! Created file at: ${paths.output}`);
  } catch (err) {
    console.error("Error building master CSV:", err);
    process.exit(1);
  }
})();
