// Configuration
const API_BASE_URL = 'http://localhost:8000';
const UPDATE_INTERVAL = 10 * 60 * 1000; // 10 minutes
const CACHE_DURATION = 2 * 60 * 1000; // 2 minutes cache validity

// Initialize Map
const map = L.map('map', {
    zoomControl: false,
    attributionControl: false
}).setView([20, 0], 3);

L.control.zoom({ position: 'topright' }).addTo(map);
L.control.attribution({ position: 'bottomright' }).addTo(map);

// Dark Matter Basemap
L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    attribution: '&copy; OpenStreetMap &copy; CARTO',
    subdomains: 'abcd',
    maxZoom: 19
}).addTo(map);

// Layer Groups
let severeMarkersLayer = L.layerGroup().addTo(map);
let moderateMarkersCluster = L.markerClusterGroup({
    iconCreateFunction: function(cluster) {
        const count = cluster.getChildCount();
        let size = 'small';
        if (count > 100) size = 'large';
        else if (count > 50) size = 'medium';
        
        return L.divIcon({
            html: '<div><span>' + count + '</span></div>',
            className: 'marker-cluster marker-cluster-moderate marker-cluster-' + size,
            iconSize: new L.Point(40, 40)
        });
    },
    spiderfyOnMaxZoom: true,
    showCoverageOnHover: false,
    zoomToBoundsOnClick: true,
    maxClusterRadius: 60,
    animate: true
}).addTo(map);

// Fetch Data
async function fetchFireData() {
    const btn = document.querySelector('.btn-primary');
    const originalBtnText = btn.innerHTML;
    btn.innerHTML = '<i class="ri-loader-4-line" style="animation: spin 1s infinite"></i> Updating...';
    btn.disabled = true;

    try {
        const severityFilter = document.getElementById('severityFilter').value;
        
        // Check cache first
        const cachedData = getCachedData('fireData');
        const cachedStats = getCachedData('fireStats');
        
        if (cachedData && cachedStats) {
            console.log('Using cached fire data');
            updateStats(cachedStats);
            updateMap(cachedData);
            
            // Hide loader on first load
            const loader = document.getElementById('loader');
            if (loader) {
                loader.style.opacity = '0';
                setTimeout(() => loader.remove(), 500);
            }
            
            btn.innerHTML = originalBtnText;
            btn.disabled = false;
            return;
        }

        // Fetch fresh data if cache is invalid
        let url = `${API_BASE_URL}/api/fires?limit=50000`;

        if (severityFilter !== 'all') {
            url += `&severity=${severityFilter}`;
        }

        const [firesRes, statsRes] = await Promise.all([
            fetch(url),
            fetch(`${API_BASE_URL}/api/stats`)
        ]);

        const fires = await firesRes.json();
        const stats = await statsRes.json();

        // Cache the data
        setCachedData('fireData', fires);
        setCachedData('fireStats', stats);

        updateStats(stats);
        updateMap(fires);

        // Hide loader on first load
        const loader = document.getElementById('loader');
        if (loader) {
            loader.style.opacity = '0';
            setTimeout(() => loader.remove(), 500);
        }

    } catch (error) {
        console.error('Error:', error);
        // alert('Connection error. Is the backend running?');
    } finally {
        btn.innerHTML = originalBtnText;
        btn.disabled = false;
    }
}

function updateStats(stats) {
    // Animate numbers
    animateValue("totalFires", parseInt(document.getElementById("totalFires").innerText) || 0, stats.total_fires, 1000);
    animateValue("severeFires", parseInt(document.getElementById("severeFires").innerText) || 0, stats.severe_fires, 1000);
    animateValue("moderateFires", parseInt(document.getElementById("moderateFires").innerText) || 0, stats.moderate_fires, 1000);

    if (stats.last_update) {
        const date = new Date(stats.last_update + 'Z');
        const timeStr = date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
        document.getElementById('lastUpdate').innerHTML = `Updated ${timeStr}`;
    }
}

function updateMap(fires) {
    severeMarkersLayer.clearLayers();
    moderateMarkersCluster.clearLayers();

    const severeFires = fires.filter(f => f.severity === 'severe');
    const moderateFires = fires.filter(f => f.severity === 'moderate');

    // Add Severe Fires
    severeFires.forEach(fire => {
        const marker = L.circleMarker([fire.latitude, fire.longitude], {
            radius: 8,
            fillColor: '#ef4444',
            color: '#fff',
            weight: 2,
            opacity: 0.5,
            fillOpacity: 0.9,
            className: 'pulse-marker'
        });
        marker.bindPopup(createPopup(fire));
        severeMarkersLayer.addLayer(marker);
    });

    // Add Moderate Fires
    moderateFires.forEach(fire => {
        const marker = L.circleMarker([fire.latitude, fire.longitude], {
            radius: 6,
            fillColor: '#f59e0b',
            color: 'transparent',
            weight: 0,
            fillOpacity: 0.7
        });
        marker.bindPopup(createPopup(fire));
        moderateMarkersCluster.addLayer(marker);
    });
}

function createPopup(fire) {
    let predictionHtml = '';
    if (fire.prediction_probability !== null && fire.prediction_probability !== undefined) {
        const probPercent = (fire.prediction_probability * 100).toFixed(1);
        const riskClass = fire.prediction_risk_level === 'HIGH' ? 'risk-high' : 
                         fire.prediction_risk_level === 'MEDIUM' ? 'risk-medium' : 'risk-low';
        
        predictionHtml = `
            <div class="popup-divider"></div>
            <div class="popup-section">
                <h4><i class="ri-brain-line"></i> AI Prediction</h4>
                <div class="popup-row">
                    <span class="popup-label">Continuation Probability</span>
                    <strong>${probPercent}%</strong>
                </div>
                <div class="popup-row">
                    <span class="popup-label">Risk Level</span>
                    <strong class="${riskClass}">${fire.prediction_risk_level}</strong>
                </div>
            </div>
        `;
    }
    
    return `
        <div class="popup-content">
            <h3><i class="ri-fire-fill"></i> ${fire.severity.toUpperCase()} FIRE</h3>
            <div class="popup-row">
                <span class="popup-label">FRP Intensity</span>
                <strong>${fire.frp ? fire.frp.toFixed(1) + ' MW' : 'N/A'}</strong>
            </div>
            <div class="popup-row">
                <span class="popup-label">Confidence</span>
                <strong>${fire.confidence === 'h' ? 'High' : 'Nominal'}</strong>
            </div>
            <div class="popup-row">
                <span class="popup-label">Satellite</span>
                <strong>${fire.satellite || 'N/A'}</strong>
            </div>
            <div class="popup-row">
                <span class="popup-label">Detected</span>
                <strong>${new Date(fire.detected_at).toLocaleTimeString()}</strong>
            </div>
            <div class="popup-row">
                <span class="popup-label">Coordinates</span>
                <strong>${fire.latitude.toFixed(3)}, ${fire.longitude.toFixed(3)}</strong>
            </div>
            ${predictionHtml}
        </div>
    `;
}

function animateValue(id, start, end, duration) {
    if (start === end) return;
    const obj = document.getElementById(id);
    const range = end - start;
    let current = start;
    const increment = end > start ? 1 : -1;
    const stepTime = Math.abs(Math.floor(duration / range));
    
    // If step time is too small, just jump to end
    if (stepTime < 10) {
        obj.innerHTML = end.toLocaleString();
        return;
    }

    const timer = setInterval(function() {
        current += increment;
        obj.innerHTML = current.toLocaleString();
        if (current == end) {
            clearInterval(timer);
        }
    }, stepTime);
}

function refreshData() {
    fetchFireData();
}

// Cache Management Functions
function setCachedData(key, data) {
    const cacheItem = {
        data: data,
        timestamp: Date.now()
    };
    try {
        const serialized = JSON.stringify(cacheItem);
        
        // Check if data is too large (>4MB)
        if (serialized.length > 4 * 1024 * 1024) {
            console.warn('Data too large to cache, skipping cache');
            return;
        }
        
        localStorage.setItem(key, serialized);
    } catch (e) {
        if (e.name === 'QuotaExceededError') {
            console.warn('LocalStorage quota exceeded, clearing cache and retrying');
            clearAllCache();
            try {
                // Retry with smaller dataset
                const smallerData = data.slice(0, Math.floor(data.length / 2));
                const smallerCache = {
                    data: smallerData,
                    timestamp: Date.now(),
                    partial: true
                };
                localStorage.setItem(key, JSON.stringify(smallerCache));
                console.log(`Cached ${smallerData.length} items (reduced from ${data.length})`);
            } catch (retryError) {
                console.error('Failed to cache even after clearing:', retryError);
            }
        } else {
            console.error('Error setting cache:', e);
        }
    }
}

function getCachedData(key) {
    try {
        const cached = localStorage.getItem(key);
        if (!cached) return null;

        const cacheItem = JSON.parse(cached);
        const age = Date.now() - cacheItem.timestamp;

        // Check if cache is still valid
        if (age < CACHE_DURATION) {
            if (cacheItem.partial) {
                console.log('Using partial cached data');
            }
            return cacheItem.data;
        }

        // Cache expired, remove it
        localStorage.removeItem(key);
        return null;
    } catch (e) {
        console.error('Error reading cache:', e);
        localStorage.removeItem(key);
        return null;
    }
}

function clearAllCache() {
    try {
        localStorage.removeItem('fireData');
        localStorage.removeItem('fireStats');
        localStorage.removeItem('predictionData');
        console.log('Cache cleared successfully');
    } catch (e) {
        console.error('Error clearing cache:', e);
    }
}

function clearOldCache() {
    clearAllCache();
}

function clearCache() {
    clearAllCache();
    console.log('Manual cache clear');
    fetchFireData();
}

// Event Listeners
document.getElementById('severityFilter').addEventListener('change', fetchFireData);

// Initial Load
fetchFireData();
setInterval(fetchFireData, UPDATE_INTERVAL);
