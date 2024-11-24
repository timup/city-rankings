// src/index.js
import { promises as fs } from 'fs';
import { join } from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import { MetropolitanAreaProcessor } from './processors/MetropolitanAreaProcessor.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

async function formatRankingOutput(city) {
    const output = [];
    output.push(`   Airport System Details:`);
    city.airports.forEach(airport => {
        output.push(`     - ${airport.code} (${airport.name})`);
        if (airport.icao) output.push(`       ICAO: ${airport.icao}`);
        output.push(`       Type: ${airport.type}`);
    });
    return output.join('\n');
}

async function main() {
    try {
        console.log('Starting city rankings processing...');

        // Initialize processor
        const processor = new MetropolitanAreaProcessor();

        // Load and process data
        console.log('Loading data from airports repository...');
        const success = await processor.loadData();

        if (!success) {
            throw new Error('Failed to load data');
        }

        // Generate rankings
        console.log('\nGenerating rankings...');
        const rankings = processor.buildCityRankings();

        // Output results
        console.log('\nTop 20 Metropolitan Areas:');
        console.log('==========================');
        
        for (let i = 0; i < Math.min(20, rankings.length); i++) {
            const city = rankings[i];
            console.log(`\n${i + 1}. ${city.name} (${city.metro_code})`);
            console.log(`   Country: ${city.country}`);
            console.log(`   Location: ${city.location.latitude.toFixed(2)}, ${city.location.longitude.toFixed(2)}`);
            console.log(await formatRankingOutput(city));
            console.log(`   Scores:`);
            console.log(`     Total Score: ${city.total_score.toFixed(2)}`);
            Object.entries(city.score_components).forEach(([key, value]) => {
                console.log(`     ${key.charAt(0).toUpperCase() + key.slice(1)}: ${value.toFixed(2)}`);
            });
        }

        // Export full rankings to CSV
        const outputDir = join(__dirname, '..', 'data');
        const outputPath = join(outputDir, 'rankings.csv');
        
        // Ensure output directory exists
        try {
            await fs.access(outputDir);
        } catch {
            await fs.mkdir(outputDir, { recursive: true });
        }

        // Export rankings
        const csvContent = processor.exportToCSV(rankings);
        await fs.writeFile(outputPath, csvContent);
        console.log(`\nFull rankings exported to: ${outputPath}`);

        // Output statistics
        console.log('\nProcessing Statistics:');
        console.log('=====================');
        console.log(`Total Metropolitan Areas Analyzed: ${processor.metroAreas.size}`);
        console.log(`Total Airports Processed: ${processor.airports.size}`);
        console.log(`Total City Codes: ${processor.cityCodes.size}`);
        
        // Score distribution analysis
        const scores = rankings.map(r => r.total_score);
        const avg = scores.reduce((a, b) => a + b, 0) / scores.length;
        const min = Math.min(...scores);
        const max = Math.max(...scores);
        
        console.log('\nScore Distribution:');
        console.log('==================');
        console.log(`Average Score: ${avg.toFixed(2)}`);
        console.log(`Minimum Score: ${min.toFixed(2)}`);
        console.log(`Maximum Score: ${max.toFixed(2)}`);

        // Region distribution
        const countryCount = new Map();
        rankings.forEach(city => {
            countryCount.set(city.country, (countryCount.get(city.country) || 0) + 1);
        });

        console.log('\nRegional Distribution:');
        console.log('====================');
        [...countryCount.entries()]
            .sort((a, b) => b[1] - a[1])
            .slice(0, 10)
            .forEach(([country, count]) => {
                console.log(`${country}: ${count} metropolitan areas`);
            });

    } catch (error) {
        console.error('Error in main process:', error);
        if (error.stack) {
            console.error('Stack trace:', error.stack);
        }
        process.exit(1);
    }
}

// Run the main process
main().catch(error => {
    console.error('Unhandled error in main process:', error);
    process.exit(1);
});