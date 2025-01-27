import fs from "fs";
import path from "path";

const DATA_PATH = path.resolve("data/processed_data/cities.json");

const validateCities = () => {
  const cities = JSON.parse(fs.readFileSync(DATA_PATH, "utf-8"));
  const errors = [];

  cities.forEach((city) => {
    if (!city.city || !city.country || !city.countryCode || !city.population) {
      errors.push(`Missing required fields for city: ${JSON.stringify(city)}`);
    }
    if (city.coordinates && (!city.coordinates.lat || !city.coordinates.lng)) {
      errors.push(`Invalid coordinates for city: ${city.city}`);
    }
  });

  if (errors.length) {
    console.error("Validation errors found:");
    errors.forEach((err) => console.error(err));
  } else {
    console.log("All cities validated successfully.");
  }
};

validateCities();
