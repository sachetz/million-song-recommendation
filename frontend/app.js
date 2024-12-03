'use strict';
const express = require('express');
const mustache = require('mustache');
const filesystem = require('fs');
const url = new URL('http://10.0.0.26:8090');
const hbase = require('hbase');
const kafka = require('kafka-node');
const bodyParser = require('body-parser');
require('dotenv').config();
const port = process.argv[3];

var hclient = hbase({
    host: url.hostname,
    port: url.port,
    protocol: url.protocol.slice(0, -1)
});

const tables = ['sachetz_ContentBasedRecs', 'sachetz_ALSRecs', 'sachetz_OngoingRecs', 'sachetz_PopularRecs'];

const app = express();
app.use(express.static('public'));
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());

app.get('/', function (req, res) {
    res.sendFile(__dirname + '/public/index.html');
});

app.post('/recommendations', async function (req, res) {
    const userId = req.body.userId;
    let allSongs = [];

    console.log(`Fetching recommendations for user: ${userId}`);

    try {
        for (let table of tables) {
            let songs = await scanTable(table, table === 'sachetz_PopularRecs' ? null : userId);
            console.log(`Fetched ${songs.length} songs from table ${table} for user ${userId}`);
            allSongs = allSongs.concat(songs);
        }

        shuffleArray(allSongs);

        console.log(`Total songs fetched for user ${userId}: ${allSongs.length}`);
        console.log('Sample of fetched songs:');
        allSongs.slice(0, 5).forEach((song, index) => {
            console.log(`Song ${index + 1}: ${song.songName} by ${song.artistName} (${song.year})`);
        });

        var template = filesystem.readFileSync("recommendations.mustache").toString();
        var html = mustache.render(template, {
            userId: userId,
            songsJson: JSON.stringify(allSongs)
        });
        res.send(html);
    } catch (error) {
        console.error('Error fetching recommendations:', error);
        res.status(500).send('Error fetching recommendations');
    }
});

function scanTable(table, userId = null) {
    return new Promise((resolve, reject) => {
        let scanner;
        if (table === 'sachetz_PopularRecs') {
            scanner = hclient.table(table).scan({
                maxVersions: 1
            });
        } else if (userId) {
            scanner = hclient.table(table).scan({
                filter: {
                    type: "PrefixFilter",
                    value: `${userId}#`
                },
                maxVersions: 1
            });
        } else {
            console.error(`User ID is required for table ${table}`);
            return resolve([]);
        }

        let songs = new Map();
        let currentSong = {};
        let currentSongId = null;

        scanner.on('data', (row) => {
            const songId = table === 'sachetz_PopularRecs' ? row.key : row.key.split('#')[1];

            if (songId !== currentSongId) {
                if (currentSongId) {
                    songs.set(currentSongId, currentSong);
                }
                currentSongId = songId;
                currentSong = { songId: songId.toString() };
            }

            if (row.column === 'details:album_name') {
                currentSong.albumName = row.$ ? row.$.toString() : '';
            } else if (row.column === 'details:artist_name') {
                currentSong.artistName = row.$ ? row.$.toString() : '';
            } else if (row.column === 'details:song_name') {
                currentSong.songName = row.$ ? row.$.toString() : '';
            } else if (row.column === 'details:year') {
                currentSong.year = row.$ ? row.$.toString() : '';
            }
        });

        scanner.on('end', () => {
            if (currentSongId) {
                songs.set(currentSongId, currentSong);
            }
            console.log(`Scan completed for table ${table}${userId ? ` and user ${userId}` : ''}`);
            console.log(`Total songs fetched: ${songs.size}`);
            resolve(Array.from(songs.values()));
        });

        scanner.on('error', (err) => {
            console.error(`Error scanning table ${table}:`, err);
            reject(err);
        });
    });
}

function shuffleArray(array) {
    for (let i = array.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [array[i], array[j]] = [array[j], array[i]];
    }
}

var Producer = kafka.Producer;
var kafkaClient = new kafka.KafkaClient({kafkaHost: process.argv[2]});
var kafkaProducer = new Producer(kafkaClient);

app.post('/rate', function (req, res) {
    const { userId, songId, rating } = req.body;

    console.log(`Received rating: User ${userId} rated song ${songId} with ${rating} stars`);

    const payload = [{
        topic: 'sachetz_user_actions_topic',
        messages: JSON.stringify({ userId, songId, rating })
    }];

    kafkaProducer.send(payload, function (err, data) {
        if (err) {
            console.error('Error sending to Kafka:', err);
            res.status(500).json({ error: 'Failed to send rating' });
        } else {
            console.log('Rating sent to Kafka:', data);
            res.json({ success: true });
        }
    });
});

app.listen(port, () => {
    console.log(`Server running on http://localhost:${port}`);
});