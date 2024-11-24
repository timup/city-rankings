// src/processors/MetropolitanAreaProcessor.js
import Papa from 'papaparse';
import { promises as fs } from 'fs';
import { join } from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

export class MetropolitanAreaProcessor {
    constructor() {
        this.metroAreas = new Map();
        this.airports = new Map();
        this.cityCodes = new Map();
        // Get the root directory (2 levels up from processor file)
        this.rootDir = join(__dirname, '..', '..');
    }

    async loadData() {
        try {
            // Load airports.csv from the submodule
            const airportsPath = join(this.rootDir, 'data', 'airports-data', 'airports.csv');
            const citiesPath = join(this.rootDir, 'data', 'airports-data', 'citycodes.csv');
            
            console.log('Reading from paths:');
            console.log('Airports:', airportsPath);
            console.log('Cities:', citiesPath);

            const airportsData = await this.readCSVFile(airportsPath);
            const citiesData = await this.readCSVFile(citiesPath);
            
            console.log(`Loaded ${airportsData.length} airports and ${citiesData.length} cities`);
            
            // Process the data
            citiesData.forEach(city => this.processCityCode(city));
            airportsData.forEach(airport => this.processAirport(airport));

            return true;
        } catch (error) {
            console.error('Error loading data:', error);
            return false;
        }
    }

    async readCSVFile(filePath) {
        try {
            const fileContent = await fs.readFile(filePath, 'utf-8');
            return new Promise((resolve, reject) => {
                Papa.parse(fileContent, {
                    header: true,
                    skipEmptyLines: true,
                    complete: (results) => {
                        console.log(`Successfully parsed ${results.data.length} rows from ${filePath}`);
                        resolve(results.data);
                    },
                    error: (error) => reject(error)
                });
            });
        } catch (error) {
            console.error(`Error reading file ${filePath}:`, error);
            throw error;
        }
    }

    processCityCode(data) {
        if (!data.code) return null;

        const cityCode = {
            code: data.code,
            name: data.name || '',
            country: data.country || '',
            type: data.type || '',
            location: {
                latitude: parseFloat(data.latitude) || 0,
                longitude: parseFloat(data.longitude) || 0
            }
        };

        this.cityCodes.set(data.code, cityCode);

        // If it's a metropolitan area, also add to metroAreas
        if (data.type === 'CC') {
            this.metroAreas.set(data.code, {
                ...cityCode,
                airports: [],
                metrics: {
                    total_passengers: 0,
                    international_routes: 0,
                    cargo_tons: 0
                }
            });
        }

        return cityCode;
    }

    processAirport(data) {
        if (!data.code) return null;

        const airport = {
            code: data.code,
            name: data.name || '',
            city_code: data.city_code,
            country: data.country || '',
            location: {
                latitude: parseFloat(data.latitude) || 0,
                longitude: parseFloat(data.longitude) || 0,
                elevation: parseFloat(data.elevation) || 0
            },
            type: data.type || '',
            timeZone: data.time_zone || '',
            icao: data.icao || ''
        };

        this.airports.set(data.code, airport);

        // Associate with metro area if exists
        if (data.city_code && this.metroAreas.has(data.city_code)) {
            const metroArea = this.metroAreas.get(data.city_code);
            if (!metroArea.airports.includes(data.code)) {
                metroArea.airports.push(data.code);
            }
        }

        return airport;
    }

    buildCityRankings() {
        const rankings = [];
        
        for (const [code, metro] of this.metroAreas) {
            // Only include metro areas with airports
            if (metro.airports.length > 0) {
                const ranking = {
                    metro_code: code,
                    name: metro.name,
                    country: metro.country,
                    airport_system: metro.airports.join('/'),
                    num_airports: metro.airports.length,
                    location: metro.location,
                    airports: metro.airports.map(code => {
                        const airport = this.airports.get(code);
                        return {
                            code: code,
                            name: airport.name,
                            type: airport.type,
                            icao: airport.icao
                        };
                    }),
                    metrics: {
                        ...metro.metrics,
                        airport_count: metro.airports.length
                    },
                    score_components: {
                        aviation: this.calculateAviationScore(metro),
                        economic: this.calculateEconomicScore(metro),
                        connectivity: this.calculateConnectivityScore(metro)
                    }
                };
                
                ranking.total_score = this.calculateTotalScore(ranking.score_components);
                rankings.push(ranking);
            }
        }

        return rankings.sort((a, b) => b.total_score - a.total_score);
    }

    calculateAviationScore(metro) {
        // Base score on number of airports and their types
        const baseScore = Math.min(100, metro.airports.length * 25);
        
        // Bonus for international airports
        const internationalAirports = metro.airports.filter(code => {
            const airport = this.airports.get(code);
            return airport && airport.type === 'AP' && this.hasInternationalRoutes(code);
        }).length;

        // Additional score for airport diversity
        const airportTypes = new Set(metro.airports.map(code => {
            const airport = this.airports.get(code);
            return airport ? airport.type : null;
        }));

        const diversityBonus = airportTypes.size * 5;

        return Math.min(100, baseScore + (internationalAirports * 10) + diversityBonus);
    }

    calculateEconomicScore(metro) {
        // This would ideally incorporate:
        // - GDP data
        // - Financial center ranking
        // - Corporate headquarters count
        // For now, using a simplified scoring based on airport system size
        const baseScore = 50; // Base economic score
        const airportBonus = metro.airports.length * 10;
        
        return Math.min(100, baseScore + airportBonus);
    }

    calculateConnectivityScore(metro) {
        // Calculate connectivity based on:
        // 1. Number of airports
        const airportCount = metro.airports.length;
        const airportScore = Math.min(50, airportCount * 10);

        // 2. Geographic location (bonus for being near other major metros)
        const locationScore = this.calculateLocationScore(metro);

        return Math.min(100, airportScore + locationScore);
    }

    calculateLocationScore(metro) {
        // Basic location score based on presence in major regions
        // This could be enhanced with actual geographic calculations
        const latitude = metro.location.latitude;
        const longitude = metro.location.longitude;

        // Bonus for being in major economic regions
        let regionScore = 0;

        // Europe
        if (latitude > 35 && latitude < 60 && longitude > -10 && longitude < 40) {
            regionScore += 30;
        }
        // North America
        else if (latitude > 25 && latitude < 50 && longitude > -130 && longitude < -60) {
            regionScore += 30;
        }
        // East Asia
        else if (latitude > 20 && latitude < 45 && longitude > 100 && longitude < 145) {
            regionScore += 30;
        }
        // Other regions get a smaller bonus
        else {
            regionScore += 15;
        }

        return regionScore;
    }

    calculateTotalScore(components) {
        const weights = {
            aviation: 0.4,
            economic: 0.3,
            connectivity: 0.3
        };

        return Object.entries(components).reduce((total, [key, value]) => {
            return total + (value * weights[key]);
        }, 0);
    }

    hasInternationalRoutes(airportCode) {
        // In a full implementation, this would check actual route data
        // For now, assume major airports have international routes
        const airport = this.airports.get(airportCode);
        return airport && airport.type === 'AP';
    }

    exportToCSV(rankings) {
        // Flatten the data for CSV export
        const flattenedRankings = rankings.map(ranking => ({
            metro_code: ranking.metro_code,
            name: ranking.name,
            country: ranking.country,
            airport_system: ranking.airport_system,
            num_airports: ranking.num_airports,
            total_score: ranking.total_score,
            aviation_score: ranking.score_components.aviation,
            economic_score: ranking.score_components.economic,
            connectivity_score: ranking.score_components.connectivity,
            latitude: ranking.location.latitude,
            longitude: ranking.location.longitude
        }));

        return Papa.unparse(flattenedRankings);
    }
}