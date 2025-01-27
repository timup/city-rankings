// scripts/format_ap_list.js

import fs from "fs";
import csv from "csv-parser";

const inputCSV = "data/2024-city-master.csv";

function apCityName(city) {
  if (!city) return "";

  // Step 1: Remove diacritics from city
  const withoutDiacritics = removeDiacritics(city);

  // Step 2: Lowercase for dictionary lookup
  const lower = withoutDiacritics.toLowerCase().trim();

  // Step 3: If we have a known override, use it
  if (apOverrides[lower]) {
    return apOverrides[lower];
  }

  // Otherwise, return the stripped version (capitalizing if you want)
  // You can choose to re-capitalize or just return as-is:
  // e.g. "kolkata" -> "Kolkata" if you want a capital letter:
  return capitalizeFirstLetter(withoutDiacritics);
}

// We can define a helper to capitalize the first letter:
function capitalizeFirstLetter(str) {
  if (!str) return str;
  return str.charAt(0).toUpperCase() + str.slice(1);
}

// The diacritic-removal function from above:
function removeDiacritics(str) {
  return str
    .normalize("NFD")
    .replace(/\p{Diacritic}+/gu, "");
}

// Our AP override dictionary:
const apOverrides = {
  "new york": "New York City",
  "washington": "Washington, D.C.",
  "saigon": "Ho Chi Minh City",
  "ho chi minh": "Ho Chi Minh City",
  "sankt-peterburg": "St. Petersburg",
  "saint petersburg": "St. Petersburg",
  "macao": "Macau",
  "kiev": "Kyiv",
  "kiev city": "Kyiv",
  "kolkata": "Kolkata", // unify Kolkāta
  // Add more rules as desired
};

function main() {
  const rows = [];

  fs.createReadStream(inputCSV)
    .pipe(csv())
    .on("data", (row) => {
      rows.push(row);
    })
    .on("end", () => {
      // Now rows[] has the CSV content in order
      // We'll assume it's already sorted by final rank
      rows.forEach((cityObj, index) => {
        const rank = index + 1;
        // 'city' column from your CSV
        const cityName = apCityName(cityObj.city?.trim() || "");
        // Print "1. New York City" style
        console.log(`${rank}. ${cityName}`);
      });
    })
    .on("error", (err) => {
      console.error("Error reading CSV:", err);
      process.exit(1);
    });
}

main();
