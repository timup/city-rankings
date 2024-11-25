import fs from "fs";
import path from "path";
import csv from "csv-parser";

const countryMap = {
  USA: "us",
  UK: "gb",
  UAE: "ae",
  DEU: "de",
  GER: "de",
  DE: "de",
  GBR: "gb",
  GB: "gb",
  FRA: "fr",
  FR: "fr",
  JPN: "jp",
  JP: "jp",
  CHN: "cn",
  CN: "cn",
  KOR: "kr",
  KR: "kr",
  SGP: "sg",
  SG: "sg",
  NLD: "nl",
  NL: "nl",
  HKG: "hk",
  HK: "hk",
  THA: "th",
  TH: "th",
  TUR: "tr",
  TR: "tr",
};

const files = {
  population: path.resolve("data/city-population/simplemaps-worldcities.csv"),
  traffic: path.resolve("data/2023-airport-traffic.csv"),
  citycodes: path.resolve("data/airports-data/citycodes.csv"),
  airports: path.resolve("data/airports-data/airports.csv"),
};

const loadCSV = (filePath) => {
  return new Promise((resolve, reject) => {
    const rows = [];
    fs.createReadStream(filePath)
      .pipe(csv())
      .on("data", (row) => rows.push(row))
      .on("end", () => resolve(rows))
      .on("error", (err) => reject(err));
  });
};

const normalizeKey = (name, country) => {
  if (!name || !country) return "";

  // Remove special characters and spaces, convert to lowercase
  const cleanName = name.toLowerCase().replace(/[^a-z0-9]/g, "");

  // Normalize country code
  const normalizedCountry = country.toLowerCase();

  return cleanName + normalizedCountry;
};

const normalizeCityCode = (code) => {
  if (!code) return "";
  return code.trim().toLowerCase();
};

const loadPopulationData = async () => {
  const populationMap = {};

  return new Promise((resolve, reject) => {
    fs.createReadStream(files.population)
      .pipe(csv())
      .on("data", (row) => {
        // Ensure we have valid data
        if (!row.city || !row.iso2 || !row.population) {
          console.warn("Invalid row in population data:", row);
          return;
        }

        const key = normalizeKey(row.city, row.iso2);
        if (key) {
          populationMap[key] = {
            city: row.city,
            country: row.iso2,
            population: parseInt(row.population, 10) || 0,
          };

          // Debug logging for important cities
          if (
            ["New York", "Frankfurt", "London", "Tokyo", "Paris"].includes(
              row.city
            )
          ) {
            console.log(
              `Loaded population for ${row.city}: ${key} -> ${row.population}`
            );
          }
        }
      })
      .on("end", () => {
        // Verify data loading
        console.log("Population data loaded. Sample entries:");
        const sampleCities = ["newyorkus", "tokyojp", "londongb", "parisfr"];
        sampleCities.forEach((key) => {
          console.log(
            `${key}: ${populationMap[key]?.population || "NOT FOUND"}`
          );
        });

        resolve(populationMap);
      })
      .on("error", (error) => {
        console.error("Error loading population data:", error);
        reject(error);
      });
  });
};

const loadCityCodes = async () => {
  const cityCodesMap = {};
  const rows = await loadCSV(files.citycodes);

  rows.forEach((row) => {
    if (!row.city_code || !row.country) return;

    const code = row.city_code.toLowerCase();
    const country = row.country.toLowerCase();
    const city = row.city?.toLowerCase() || "";

    if (!cityCodesMap[code]) {
      cityCodesMap[code] = [];
    }

    // Create normalized key for the city
    const key = normalizeKey(city || code, country);
    if (key) {
      cityCodesMap[code].push({
        key,
        city: row.city || "",
        country: row.country,
      });
    }
  });

  return cityCodesMap;
};

const loadTrafficData = async () => {
  const trafficMap = {};
  const cityCodesMap = await loadCityCodes();

  // Add missing traffic data for major cities
  const missingTrafficData = {
    bangkokth: {
      totalPassengers: 65800000,
      internationalPassengers: 52000000,
      cargoTraffic: 1200000,
    },
    istanbultr: {
      totalPassengers: 64500000,
      internationalPassengers: 48000000,
      cargoTraffic: 1800000,
    },
    delhiin: {
      totalPassengers: 59800000,
      internationalPassengers: 15800000,
      cargoTraffic: 950000,
    },
    guangzhoucn: {
      totalPassengers: 43500000,
      internationalPassengers: 8500000,
      cargoTraffic: 1800000,
    },
    mexicocitymx: {
      totalPassengers: 48416008, // Updated from CSV
      internationalPassengers: 20000000,
      cargoTraffic: 80000,
    },
    dohaqat: {
      totalPassengers: 45916098, // Updated from CSV
      internationalPassengers: 19000000,
      cargoTraffic: 60000,
    },
  };

  // Merge missing traffic data into trafficMap
  Object.entries(missingTrafficData).forEach(([key, data]) => {
    trafficMap[key] = data;
  });

  // Process regular traffic data
  const rows = await loadCSV(files.traffic);
  rows.forEach((row) => {
    const cityName = row["City/Airport"].split("/")[0].trim();
    const cityCode = row["IATA City Code"];

    // Create traffic data object
    const trafficData = {
      totalPassengers:
        parseInt(row["Total Passengers 2023"].replace(/,/g, ""), 10) || 0,
      internationalPassengers:
        parseInt(row["International Passengers 2023"].replace(/,/g, ""), 10) ||
        0,
      cargoTraffic:
        parseInt(row["Cargo Traffic 2023 (tons)"].replace(/,/g, ""), 10) || 0,
    };

    // Special handling for major cities with adjusted weights
    const specialCases = {
      "New York": { key: "newyorkus", weight: 1.15 },
      "London": { key: "londongb", weight: 0.925 },
      "Tokyo": { key: "tokyojp", weight: 1.05 },
      "Paris": { key: "parisfr", weight: 1.0 },
      "Hong Kong": { key: "hongkonghk", weight: 0.92 },
      "Shanghai": { key: "shanghaicn", weight: 0.98 },
      "Dubai": { key: "dubaiae", weight: 1.0 },
      "Beijing": { key: "beijingcn", weight: 0.94 },
      "Seoul": { key: "seoulkr", weight: 1.0 },
      "Singapore": { key: "singaporesg", weight: 1.0 },
      "Washington": { key: "washingtonus", weight: 1.0 },
      "Rome": { key: "romeit", weight: 1.0 },
    };

    if (specialCases[cityName]) {
      const { key, weight } = specialCases[cityName];
      trafficMap[key] = {
        totalPassengers: Math.round(trafficData.totalPassengers * weight),
        internationalPassengers: Math.round(
          trafficData.internationalPassengers * weight
        ),
        cargoTraffic: Math.round(trafficData.cargoTraffic * weight),
      };
    }

    // Normal processing for other cities
    if (cityCode) {
      const normalizedCities = cityCodesMap[cityCode.toLowerCase()] || [];
      normalizedCities.forEach(({ key }) => {
        if (!specialCases[cityName]) {
          // Only if not already handled as special case
          trafficMap[key] = trafficData;
        }
      });
    }
  });

  return trafficMap;
};

(async () => {
  try {
    console.log("Loading data...");
    const [populationData, trafficData, cityCodesData, airportsData] =
      await Promise.all([
        loadCSV(files.population),
        loadCSV(files.traffic),
        loadCSV(files.citycodes),
        loadCSV(files.airports),
      ]);

    console.log("Processing city codes...");
    const cityCodeMap = cityCodesData.reduce((map, city) => {
      const normalizedCode = city.code?.trim().toLowerCase();
      const normalizedCity = city.city?.trim().toLowerCase() || normalizedCode; // Fallback to code if city is empty
      if (normalizedCode && normalizedCity) {
        map[normalizedCode] = normalizedCity;
        console.log(`Mapped city code: ${normalizedCode} -> ${normalizedCity}`);
      } else {
        console.log(
          `Skipped invalid city code mapping: ${JSON.stringify(city)}`
        );
      }
      return map;
    }, {});

    console.log(`Total mapped city codes: ${Object.keys(cityCodeMap).length}`);

    console.log("Processing population data...");
    const populationMap = await loadPopulationData();

    console.log("Population data keys for major cities:");
    [
      "tokyo",
      "newyork",
      "london",
      "paris",
      "singapore",
      "dubai",
      "shanghai",
      "seoul",
      "chicago",
      "frankfurt",
    ].forEach((city) => {
      const matches = Object.keys(populationMap).filter((k) =>
        k.toLowerCase().includes(city)
      );
      console.log(`${city}: ${matches.join(", ")}`);
    });

    console.log("Population data for key cities:");
    ["delhi", "mumbai", "jakarta", "los angeles"].forEach((city) => {
      const matches = Object.entries(populationMap)
        .filter(([key, data]) => data.city.toLowerCase().includes(city))
        .map(([key, data]) => `${data.city}: ${data.population}`);
      console.log(`${city}: ${matches.join(", ")}`);
    });

    console.log("Population data for major cities:");
    [
      "New York",
      "London",
      "Tokyo",
      "Paris",
      "Singapore",
      "Hong Kong",
      "Shanghai",
      "Dubai",
      "Beijing",
      "Los Angeles",
      "Seoul",
      "Chicago",
      "Frankfurt",
      "Amsterdam",
    ].forEach((cityName) => {
      const key = Object.keys(populationMap).find(
        (k) => populationMap[k].city.toLowerCase() === cityName.toLowerCase()
      );
      console.log(
        `${cityName}: ${key ? populationMap[key].population : "NOT FOUND"}`
      );
    });

    console.log("Population data debug:");
    [
      ["Frankfurt", "de"],
      ["New York", "us"],
      ["Frankfurt am Main", "de"],
      ["New York City", "us"],
    ].forEach(([city, country]) => {
      const key = normalizeKey(city, country);
      console.log(
        `${city}, ${country} -> ${key} -> ${
          populationMap[key]?.city || "NOT FOUND"
        }`
      );
    });

    console.log("Processing traffic data...");
    const unmatchedCodes = new Set();
    const trafficMap = await loadTrafficData();

    console.log("Unmatched City Codes:", Array.from(unmatchedCodes).join(", "));

    console.log("Generating rankings...");
    // Define top global cities and their importance multipliers
    const globalCityMultipliers = {
      "New York": 5.0,
      "London": 4.9,
      "Tokyo": 4.8,
      "Paris": 4.6,
      "Washington": 8.4,
      "Singapore": 4.0,
      "Hong Kong": 4.0,
      "Shanghai": 3.5,
      "Dubai": 3.8,
      "Beijing": 3.5,
      "Los Angeles": 3.2,
      "Seoul": 3.0,
      "Chicago": 2.8,
      "Bangkok": 3.0,
      "Istanbul": 2.8,
      "Delhi": 2.6,
      "Guangzhou": 2.4
    };

    const rankings = Object.keys(populationMap).map((key) => {
      const city = populationMap[key]?.city;
      const population = populationMap[key]?.population || 0;
      const traffic = trafficMap[key] || {
        totalPassengers: 0,
        internationalPassengers: 0,
        cargoTraffic: 0,
      };

      // Calculate base score with adjusted weights
      let score = (
        population * 0.4 +                           // Population impact
        traffic.totalPassengers * 0.2 +             // Total passengers
        traffic.internationalPassengers * 0.3 +      // International traffic
        traffic.cargoTraffic * 20 +                 // Cargo traffic
        (city === "Washington" ? 1000000 : 0)       // Political capital bonus
      ) / 10;

      // Apply global city multiplier if applicable
      const multiplier = globalCityMultipliers[city] || 1.0;
      score *= multiplier;

      return {
        city: city || "Unknown",
        country: populationMap[key]?.country || "Unknown",
        population,
        totalPassengers: traffic.totalPassengers,
        internationalPassengers: traffic.internationalPassengers,
        cargoTraffic: traffic.cargoTraffic,
        score: Math.round(score),  // Round to whole numbers
        hasTrafficData: traffic.totalPassengers > 0
      };
    });

    rankings.sort((a, b) => b.score - a.score);

    const outputPath = path.resolve("output/ranked_cities.csv");
    console.log(`Writing results to ${outputPath}...`);
    const outputData = [
      "City,Country,Population,Total Passengers,International Passengers,Cargo Traffic,Score,Has Traffic Data",
    ]
      .concat(
        rankings.map(
          (row) =>
            `${row.city},${row.country},${row.population},${row.totalPassengers},${row.internationalPassengers},${row.cargoTraffic},${row.score},${row.hasTrafficData}`
        )
      )
      .join("\n");
    fs.writeFileSync(outputPath, outputData, "utf8");

    console.log("Rankings generated successfully!");

    // After loading population data
    const missingMajorCities = [
      ["Frankfurt", "DE"],
      ["New York", "US"],
      ["London", "GB"],
    ].filter(([city, country]) => {
      const key = normalizeKey(city, country);
      return !populationMap[key];
    });

    if (missingMajorCities.length > 0) {
      console.warn(
        "Missing major cities in population data:",
        missingMajorCities
          .map(([city, country]) => `${city} (${country})`)
          .join(", ")
      );
    }
  } catch (err) {
    console.error("Error:", err);
  }
})();
