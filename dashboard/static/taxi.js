const map = L.map('map', {preferCanvas: true,  closePopupOnClick: false}).setView([39.9167, 116.3975], 11);
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png').addTo(map);

// 10km forbidden city borders (green circle)
L.circle([39.9167, 116.3975], { radius: 10000, color: 'green', fillColor: '#00ff00', fillOpacity: 0.1 }).addTo(map);
// 15km Discard area (red dashed circle)
L.circle([39.9167, 116.3975], { radius: 15000, color: 'red', weight: 1, dashArray: '5, 5', fillOpacity: 0 }).addTo(map);

const taxiMarkers = {};
const taxiRequestTokens = {};
const animationFrames = {};
const activeAnimations = {};
const ANIMATION_DURATION = 4800;
const DASHBOARD_MAX_RADIUS_KM = 15;

function focusOnTaxi(taxiId) {
    const marker = taxiMarkers[taxiId];
    if (marker) {
        const targetLatLng = marker.getLatLng();
        map.flyTo(targetLatLng, 14, {
            animate: true,
            duration: 1.5
        });
        marker.openPopup();
    }
}

async function getRouteFromOSRM(startLatLng, endLatLng) {
    const url = `https://router.project-osrm.org/route/v1/driving/${startLatLng.lng},${startLatLng.lat};${endLatLng[1]},${endLatLng[0]}?overview=simplified&geometries=geojson`;

    try {
        const response = await fetch(url);
        if (response.status === 429) return null;

        const data = await response.json();
        if (data.routes && data.routes.length > 0) {
            return {
                coords: data.routes[0].geometry.coordinates.map(c => [c[1], c[0]]),
                osrmDistance: data.routes[0].distance
            };
        }
    } catch (e) {}

    return null;
}

function animateMarkerAlongPath(marker, pathCoords, duration, taxiId) {
    if (!pathCoords || pathCoords.length < 2) return;

    if (animationFrames[taxiId]) {
        cancelAnimationFrame(animationFrames[taxiId]);
    }

    let startTime = null;

    function step(currentTime) {
        if (!startTime) startTime = currentTime;
        const progress = Math.min((currentTime - startTime) / duration, 1);

        const totalSegments = pathCoords.length - 1;
        const segmentFloat = progress * totalSegments;
        const segmentIndex = Math.min(Math.floor(segmentFloat), totalSegments - 1);
        const segmentProgress = segmentFloat - segmentIndex;

        const p1 = pathCoords[segmentIndex];
        const p2 = pathCoords[segmentIndex + 1];

        const currentLat = p1[0] + (p2[0] - p1[0]) * segmentProgress;
        const currentLng = p1[1] + (p2[1] - p1[1]) * segmentProgress;

        marker.setLatLng([currentLat, currentLng]);

        if (progress < 1) {
            animationFrames[taxiId] = requestAnimationFrame(step);
        } else {
            delete animationFrames[taxiId];
        }
    }
    animationFrames[taxiId] = requestAnimationFrame(step);
}

function animateMarker(marker, startLat, startLng, endLat, endLng, taxiId) {
    const duration = ANIMATION_DURATION;
    const startTime = performance.now();

    if (activeAnimations[taxiId]) {
        cancelAnimationFrame(activeAnimations[taxiId]);
    }

    function step(currentTime) {
        const progress = Math.min((currentTime - startTime) / duration, 1);

        const lat = startLat + (endLat - startLat) * progress;
        const lng = startLng + (endLng - startLng) * progress;

        marker.setLatLng([lat, lng]);

        if (progress < 1) {
            activeAnimations[taxiId] = requestAnimationFrame(step);
        } else {
            delete activeAnimations[taxiId];
        }
    }

    activeAnimations[taxiId] = requestAnimationFrame(step);
}

function pickRouteFromCurrentPosition(currentLatLng, routeCoords, endLatLng) {
    if (!routeCoords || routeCoords.length === 0) {
        return [[currentLatLng.lat, currentLatLng.lng], endLatLng];
    }

    let nearestIdx = 0;
    let bestDistSq = Infinity;

    for (let i = 0; i < routeCoords.length; i++) {
        const dLat = routeCoords[i][0] - currentLatLng.lat;
        const dLng = routeCoords[i][1] - currentLatLng.lng;
        const distSq = dLat * dLat + dLng * dLng;
        if (distSq < bestDistSq) {
            bestDistSq = distSq;
            nearestIdx = i;
        }
    }

    const remaining = routeCoords.slice(nearestIdx);
    const path = [[currentLatLng.lat, currentLatLng.lng], ...remaining];

    const last = path[path.length - 1];
    if (!last || last[0] !== endLatLng[0] || last[1] !== endLatLng[1]) {
        path.push(endLatLng);
    }

    return path;
}

// web socket
const socketUrl = window.location.hostname === 'localhost' ? 'ws://localhost:5050/taxi': `ws://${window.location.host}/taxi`;
const ws = new WebSocket(socketUrl);

ws.onmessage = function (event) {
    const data = JSON.parse(event.data);

    const visibleTaxis = data.taxis.filter(taxi => (taxi.distance_to_center_km || 0) <= DASHBOARD_MAX_RADIUS_KM);

    document.getElementById("currently-driving").innerText = visibleTaxis.length;
    document.getElementById("overall-distance").innerText = data.overall_distance + " km";

    const activeMovingIds = new Set(visibleTaxis.map(t => t.id));

    const areaUl = document.getElementById("area-violations");
    areaUl.innerHTML = data.area_violations
        .filter(id => activeMovingIds.has(Number(id)))
        .map(id => `<li class="area clickable-alert" onclick="focusOnTaxi(${id})">⚠️ Taxi ${id}</li>`)
        .join("");

    const speedUl = document.getElementById("speeding-incidents");
    speedUl.innerHTML = data.speeding_incidents
        .filter(id => activeMovingIds.has(Number(id)))
        .map(id => `<li class="clickable-alert" onclick="focusOnTaxi(${id})">⚡ Taxi ${id}</li>`)
        .join("");

    const currentIds = visibleTaxis.map(t => t.id);
    for (let id in taxiMarkers) {
        if (!currentIds.includes(id)) {
            map.removeLayer(taxiMarkers[id]);
            delete taxiMarkers[id];
            if (animationFrames[id]) {
                cancelAnimationFrame(animationFrames[id]);
                delete animationFrames[id];
            }
            if (activeAnimations[id]) {
                cancelAnimationFrame(activeAnimations[id]);
                delete activeAnimations[id];
            }
            delete taxiRequestTokens[id];
        }
    }

    visibleTaxis.forEach(taxi => {
        const debugContent = `
            <div style="font-family: Arial, sans-serif; font-size: 12px; line-height: 1.4;">
                <b style="color: #007bff;">🚖 Taxi ID: ${taxi.id}</b><br>
                <hr style="margin: 4px 0; border: 0; border-top: 1px solid #eee;">
                <b>Speed:</b> ${taxi.speed || 0} km/h<br>
                <b>Total Distance:</b> ${taxi.total_distance_km || 0} km<br>
                <b>Distance to Center:</b> ${taxi.distance_to_center_km || 0} km<br>
                <b>Area Violation:</b> ${taxi.area_violation === 1 ? '<span style="color:red;">YES</span>' : 'NO'}<br>
                <b>Speed Violation:</b> ${taxi.speed_violation === 1 ? '<span style="color:red;">YES</span>' : 'NO'}<br>
                <span style="font-size: 10px; color: #999;">Lat: ${taxi.lat.toFixed(4)}, Lon: ${taxi.lon.toFixed(4)}</span>
            </div>
        `;

        if (taxiMarkers[taxi.id]) {
            const startLatLng = taxiMarkers[taxi.id].getLatLng();
            const endLatLng = [taxi.lat, taxi.lon];
            const jumpDistance = map.distance(startLatLng, endLatLng);

            if (taxiMarkers[taxi.id].getPopup()) {
                taxiMarkers[taxi.id].setPopupContent(debugContent);
            }

            if (jumpDistance > 30000) {
                taxiMarkers[taxi.id].setLatLng(endLatLng);
            }  else if (jumpDistance > 5) {
                taxiRequestTokens[taxi.id] = (taxiRequestTokens[taxi.id] || 0) + 1;
                const currentToken = taxiRequestTokens[taxi.id];

                getRouteFromOSRM(startLatLng, [taxi.lat, taxi.lon]).then(routeInfo => {
                    if (!taxiMarkers[taxi.id]) return;
                    if (taxiRequestTokens[taxi.id] !== currentToken) return;

                    const liveStart = taxiMarkers[taxi.id].getLatLng();
                    const liveJumpDistance = map.distance(liveStart, endLatLng);

                    if (liveJumpDistance <= 0.1) return;

                    if (!routeInfo || routeInfo.osrmDistance > jumpDistance * 5) {
                        animateMarker(taxiMarkers[taxi.id], liveStart.lat, liveStart.lng, taxi.lat, taxi.lon, taxi.id);
                    } else {
                        const livePath = pickRouteFromCurrentPosition(liveStart, routeInfo.coords, endLatLng);
                        animateMarkerAlongPath(taxiMarkers[taxi.id], livePath, ANIMATION_DURATION, taxi.id);
                    }
                });
            } else if (jumpDistance > 0.1) {
                animateMarker(taxiMarkers[taxi.id], startLatLng.lat, startLatLng.lng, taxi.lat, taxi.lon, taxi.id);
            }
        } else {
            // create blue circle marker for each taxi
            taxiMarkers[taxi.id] = L.circleMarker([taxi.lat, taxi.lon], {
                radius: 6, fillColor: '#007bff', color: '#fff', weight: 2, fillOpacity: 1
            }).addTo(map).bindPopup(debugContent, {
                autoClose: false,
                closeOnClick: false
            });
        }
    });
};