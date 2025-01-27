import fs from "fs";
import path from "path";
import axios from "axios";
import csv from "csv-parser";

const RAW_DATA_PATH = path.resolve("data/raw_data/simplemaps-worldcities.csv");
const OUTPUT_PATH = path.resolve("data/processed_data/cities.json");
const GEO_NAMES_USERNAME = "flightlines"; // Replace with your username

// Fetch GeoNames data
const fetchGeoNamesData = async (city, countryCode) => {
  const url = `http://api.geonames.org/searchJSON?q=${city}&country=${countryCode}&maxRows=1&username=${GEO_NAMES_USERNAME}`;
  try {
    const response = await axios.get(url);
    const geoData = response.data.geonames[0];
    return geoData
      ? {
          geoNamesId: geoData.geonameId,
          lat: parseFloat(geoData.lat),
          lng: parseFloat(geoData.lng),
          timezone: geoData.timezone,
        }
      : {};
  } catch (err) {
    console.error(`GeoNames fetch error for ${city}, ${countryCode}:`, err);
    return {};
  }
};

// Process cities data
const processCities = async () => {
  const cities = [];
  return new Promise((resolve, reject) => {
    fs.createReadStream(RAW_DATA_PATH)
      .pipe(csv())
      .on("data", async (row) => {
        const { city, country, countryCode, population } = row;
        console.log(`Processing: ${city}, ${countryCode}`);
        const geoData = await fetchGeoNamesData(city, countryCode);

        cities.push({
          id: `${city.toLowerCase().replace(/ /g, "_")}-${countryCode.toLowerCase()}`,
          city,
          country,
          countryCode,
          population: parseInt(population, 10),
          coordinates: geoData.lat ? { lat: geoData.lat, lng: geoData.lng } : null,
          geoNamesId: geoData.geoNamesId || null,
          timezone: geoData.timezone || null,
        });
      })
      .on("end", () => resolve(cities))
      .on("error", (err) => reject(err));
  });
};

// Main function
(async () => {
  const cities = await processCities();
  fs.writeFileSync(OUTPUT_PATH, JSON.stringify(cities, null, 2));
  console.log(`Processed city data saved to ${OUTPUT_PATH}`);
})();
