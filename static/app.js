class FuturesArbitrageScanner {
    constructor() {
        this.currentSymbol = 'BTCUSDT';
        this.sources = new Map();
        this.priceHistory = new Map();
        this.arbitrageOpportunities = [];
        this.currentSpreads = new Map();
        this.maxHistoryPoints = 500; // Reduced from 1000
        this.maxOpportunities = 25; // Reduced from 50
        this.connectedSources = new Set();
        this.currentSort = { field: 'timestamp', direction: 'desc' };
        this.minProfitFilter = 0.05;
        
        // Source visibility settings with localStorage persistence
        this.enabledSources = this.loadEnabledSources();
        
        this.chart = null;
        this.chartSeries = new Map(); // Map to store series for each source
        this.ws = null;
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 10;
        
        // Performance optimization: throttling
        this.chartUpdatePending = false;
        this.sourceUpdatePending = false;
        this.opportunitiesUpdatePending = false;
        this.spreadsUpdatePending = false;
        
        // Realtime tracking
        this.isAtRealtime = true;
        this.lastUserScrollTime = 0;
        
        // WebSocket message batching
        this.messageQueue = [];
        this.processingMessages = false;
        
        this.init();
    }

    // Load enabled sources from localStorage
    loadEnabledSources() {
        const defaultSources = {
            'binance_futures': true,
            'bybit_futures': true,
            'hyperliquid_futures': true,
            'kraken_futures': true,
            'okx_futures': true,
            'gate_futures': true,
            'paradex_futures': true,
            'bingx_futures': true,
            'binance_spot': true,
            'bingx_spot': true,
            'bybit_spot': true,
            'pyth': true
        };
        
        try {
            const stored = localStorage.getItem('enabledSources');
            return stored ? { ...defaultSources, ...JSON.parse(stored) } : defaultSources;
        } catch (error) {
            console.warn('Failed to load enabled sources from localStorage:', error);
            return defaultSources;
        }
    }
    
    // Save enabled sources to localStorage
    saveEnabledSources() {
        try {
            localStorage.setItem('enabledSources', JSON.stringify(this.enabledSources));
        } catch (error) {
            console.warn('Failed to save enabled sources to localStorage:', error);
        }
    }
    
    // Toggle source visibility (now mainly used by non-UI code)
    toggleSource(source) {
        const oldState = this.enabledSources[source];
        this.enabledSources[source] = !this.enabledSources[source];
        const newState = this.enabledSources[source];
        console.log(`Toggle ${source}: ${oldState} -> ${newState}`);
        
        this.saveEnabledSources();
        
        // Update all components immediately
        this.updateSourceList();
        this.performChartUpdate(); // Force immediate chart update, bypassing throttling
        this.updateSpreadsMatrix();
        this.updateOpportunitiesTable();
    }
    
    // Update source item opacity without full HTML recreation
    updateSourceVisibility() {
        const sourceItems = document.querySelectorAll('.source-item');
        sourceItems.forEach(item => {
            const checkbox = item.querySelector('input[type="checkbox"]');
            if (checkbox && checkbox.dataset.source) {
                const source = checkbox.dataset.source;
                const isEnabled = this.isSourceEnabled(source);
                item.style.opacity = isEnabled ? '1' : '0.4';
            }
        });
    }
    
    // Check if source is enabled
    isSourceEnabled(source) {
        return this.enabledSources[source] !== false;
    }

    // Smart price formatting based on price value
    formatPrice(price) {
        if (price >= 1000) {
            return price.toFixed(2);
        } else if (price >= 100) {
            return price.toFixed(3);
        } else if (price >= 10) {
            return price.toFixed(4);
        } else if (price >= 1) {
            return price.toFixed(5);
        } else {
            return price.toFixed(6);
        }
    }

    init() {
        this.setupEventListeners();
        this.setupSourceCheckboxDelegation();
        this.setupChart();
        this.connectWebSocket();
        this.setupOpportunitiesTable();
    }

    setupEventListeners() {
        const symbolSelect = document.getElementById('symbolSelect');
        symbolSelect.addEventListener('change', (e) => {
            this.changeSymbol(e.target.value.toUpperCase());
        });

        window.addEventListener('resize', () => {
            if (this.chart) {
                this.chart.applyOptions({
                    width: this.getChartWidth(),
                    height: this.getChartHeight()
                });
            }
        });

        // Opportunities table event listeners
        const minProfitFilter = document.getElementById('minProfitFilter');
        minProfitFilter.addEventListener('input', (e) => {
            this.minProfitFilter = parseFloat(e.target.value) || 0;
            this.updateOpportunitiesTable();
            this.updateSpreadsMatrix(); // Update matrix highlighting
        });

        const clearButton = document.getElementById('clearOpportunities');
        clearButton.addEventListener('click', () => {
            this.arbitrageOpportunities = [];
            this.updateOpportunitiesTable();
        });

        // Go to realtime button
        const goToRealtimeBtn = document.getElementById('goToRealtimeBtn');
        goToRealtimeBtn.addEventListener('click', () => {
            if (this.chart) {
                this.chart.timeScale().scrollToRealTime();
                this.isAtRealtime = true;
                this.lastUserScrollTime = 0;
            }
        });
    }
    
    setupSourceCheckboxDelegation() {
        const sourceList = document.getElementById('sourceList');
        
        sourceList.addEventListener('click', (event) => {
            if (event.target.type === 'checkbox' && event.target.dataset.source) {
                const source = event.target.dataset.source;
                console.log(`Checkbox click event for source: ${source}`);
                
                // Toggle state immediately without HTML recreation
                this.enabledSources[source] = !this.enabledSources[source];
                console.log(`Toggle ${source}: -> ${this.enabledSources[source]}`);
                
                // Update checkbox state immediately
                event.target.checked = this.enabledSources[source];
                
                // Save and update other components
                this.saveEnabledSources();
                this.performChartUpdate();
                this.updateSpreadsMatrix();
                this.updateOpportunitiesTable();
                
                // Update source list opacity without full recreation
                this.updateSourceVisibility();
            }
        });
    }

    setupChart() {
        const chartContainer = document.getElementById('chart');
        
        // Create TradingView chart
        this.chart = LightweightCharts.createChart(chartContainer, {
            width: this.getChartWidth(),
            height: this.getChartHeight(),
            layout: {
                background: { color: '#0f0f0f' },
                textColor: '#e0e0e0',
                fontSize: 11,
                fontFamily: 'JetBrains Mono, Monaco, Consolas, monospace',
                attributionLogo: false
            },
            grid: {
                vertLines: { color: '#222' },
                horzLines: { color: '#222' },
            },
            crosshair: {
                mode: LightweightCharts.CrosshairMode.Normal,
            },
            rightPriceScale: {
                borderColor: '#444',
                textColor: '#888',
            },
            timeScale: {
                borderColor: '#444',
                textColor: '#888',
                timeVisible: true,
                secondsVisible: false,
            },
            handleScroll: {
                mouseWheel: true,
                pressedMouseMove: true,
            },
            handleScale: {
                axisPressedMouseMove: true,
                mouseWheel: true,
                pinch: true,
            },
        });

        // Define source configurations
        const sourceConfigs = [
            { key: 'binance_futures', label: 'Binance Futures', color: '#f0b90b', lineStyle: LightweightCharts.LineStyle.Solid },
            { key: 'bybit_futures', label: 'Bybit Futures', color: '#f7931a', lineStyle: LightweightCharts.LineStyle.Solid },
            { key: 'hyperliquid_futures', label: 'Hyperliquid Futures', color: '#97FCE4', lineStyle: LightweightCharts.LineStyle.Solid },
            { key: 'kraken_futures', label: 'Kraken Futures', color: '#5a5aff', lineStyle: LightweightCharts.LineStyle.Solid },
            { key: 'okx_futures', label: 'OKX Futures', color: '#1890ff', lineStyle: LightweightCharts.LineStyle.Solid },
            { key: 'gate_futures', label: 'Gate.io Futures', color: '#6c5ce7', lineStyle: LightweightCharts.LineStyle.Solid },
            { key: 'paradex_futures', label: 'Paradex Futures', color: '#ff6b6b', lineStyle: LightweightCharts.LineStyle.Solid },
            { key: 'bingx_futures', label: 'BingX Futures', color: '#1E3A8A', lineStyle: LightweightCharts.LineStyle.Solid },
            { key: 'binance_spot', label: 'Binance Spot', color: '#f0b90b', lineStyle: LightweightCharts.LineStyle.Dashed },
            { key: 'bingx_spot', label: 'BingX Spot', color: '#3B82F6', lineStyle: LightweightCharts.LineStyle.Dashed },
            { key: 'bybit_spot', label: 'Bybit Spot', color: '#f7931a', lineStyle: LightweightCharts.LineStyle.Dashed },
            { key: 'pyth', label: 'Pyth Oracle', color: '#00ff88', lineStyle: LightweightCharts.LineStyle.Dotted },
        ];

        // Create line series for each source
        sourceConfigs.forEach(config => {
            const series = this.chart.addLineSeries({
                color: config.color,
                lineWidth: 2,
                lineStyle: config.lineStyle,
                crosshairMarkerVisible: true,
                crosshairMarkerRadius: 4,
                crosshairMarkerBorderColor: config.color,
                crosshairMarkerBackgroundColor: config.color,
                lastValueVisible: true,
                priceLineVisible: false,
                title: config.label,
            });
            
            this.chartSeries.set(config.key, series);
        });


        // Track user interactions to determine if we should auto-scroll
        this.chart.timeScale().subscribeVisibleTimeRangeChange(() => {
            this.lastUserScrollTime = Date.now();
            this.isAtRealtime = false;
            
            // Reset realtime flag after some time of inactivity
            setTimeout(() => {
                if (Date.now() - this.lastUserScrollTime > 5000) {
                    this.isAtRealtime = true;
                }
            }, 5000);
        });

        this.updateChartTitle();
    }


    getChartWidth() {
        const chartContainer = document.getElementById('chart');
        return chartContainer ? Math.max(chartContainer.clientWidth - 40, 400) : 800;
    }

    getChartHeight() {
        const chartContainer = document.getElementById('chart');
        if (!chartContainer) return 400;
        
        const containerHeight = chartContainer.clientHeight;
        return Math.max(containerHeight - 40, 300);
    }


    connectWebSocket() {
        const wsStatus = document.getElementById('wsStatus');
        const wsStatusText = document.getElementById('wsStatusText');
        
        wsStatus.className = 'status-dot disconnected';
        wsStatusText.textContent = 'Connecting...';

        try {
            this.ws = new WebSocket(`ws://${window.location.host}/ws`);
            
            this.ws.onopen = () => {
                console.log('WebSocket connected');
                wsStatus.className = 'status-dot connected';
                wsStatusText.textContent = 'Connected';
                this.reconnectAttempts = 0;
            };

            this.ws.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);
                    this.queueMessage(data);
                } catch (error) {
                    console.error('Error parsing WebSocket message:', error);
                }
            };

            this.ws.onclose = () => {
                console.log('WebSocket disconnected');
                wsStatus.className = 'status-dot disconnected';
                wsStatusText.textContent = 'Disconnected';
                this.scheduleReconnect();
            };

            this.ws.onerror = (error) => {
                console.error('WebSocket error:', error);
                wsStatus.className = 'status-dot disconnected';
                wsStatusText.textContent = 'Error';
            };

        } catch (error) {
            console.error('Failed to create WebSocket connection:', error);
            this.scheduleReconnect();
        }
    }

    scheduleReconnect() {
        if (this.reconnectAttempts < this.maxReconnectAttempts) {
            this.reconnectAttempts++;
            const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 30000);
            console.log(`Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts})`);
            setTimeout(() => this.connectWebSocket(), delay);
        }
    }

    queueMessage(data) {
        this.messageQueue.push(data);
        if (!this.processingMessages) {
            this.processingMessages = true;
            setTimeout(() => this.processMessageQueue(), 50);
        }
    }
    
    processMessageQueue() {
        const messagesToProcess = this.messageQueue.splice(0);
        
        // Group messages by type to batch similar operations
        const priceUpdates = [];
        const arbitrageOpportunities = [];
        const spreadsUpdates = [];
        const otherMessages = [];
        
        messagesToProcess.forEach(data => {
            if (data.type === 'price_update') {
                priceUpdates.push(data);
            } else if (data.type === 'arbitrage') {
                arbitrageOpportunities.push(data);
            } else if (data.type === 'spreads') {
                spreadsUpdates.push(data);
            } else {
                otherMessages.push(data);
            }
        });
        
        // Process price updates in batch
        if (priceUpdates.length > 0) {
            this.handleBatchedPriceUpdates(priceUpdates);
        }
        
        // Process arbitrage opportunities
        arbitrageOpportunities.forEach(data => {
            this.handleArbitrageOpportunity(data.opportunity);
        });
        
        // Process latest spreads update only
        if (spreadsUpdates.length > 0) {
            this.handleSpreadsUpdate(spreadsUpdates[spreadsUpdates.length - 1]);
        }
        
        // Process other messages normally
        otherMessages.forEach(data => {
            this.handleWebSocketMessage(data);
        });
        
        this.processingMessages = false;
        
        // Schedule UI updates
        this.updateSourceList();
        this.updateChart();
    }
    
    handleBatchedPriceUpdates(priceUpdates) {
        priceUpdates.forEach(data => {
            if (data.symbol === this.currentSymbol) {
                this.updateSourcePrice(data.source, data.price);
                this.addPriceToHistory(data.source, data.price, data.timestamp);
            }
        });
    }

    handleWebSocketMessage(data) {
        if (data.type === 'prices') {
            this.updatePrices(data.prices);
        } else if (data.type === 'price_update') {
            this.handlePriceUpdate(data);
        } else if (data.type === 'arbitrage') {
            this.handleArbitrageOpportunity(data.opportunity);
        } else if (data.type === 'spreads') {
            this.handleSpreadsUpdate(data);
        }
    }

    updatePrices(prices) {
        for (const [symbol, sourcePrices] of Object.entries(prices)) {
            if (symbol === this.currentSymbol) {
                for (const [source, price] of Object.entries(sourcePrices)) {
                    this.updateSourcePrice(source, price);
                    this.addPriceToHistory(source, price);
                }
                // UI updates will be handled by processMessageQueue
                break;
            }
        }
    }

    handlePriceUpdate(data) {
        if (data.symbol === this.currentSymbol) {
            this.updateSourcePrice(data.source, data.price);
            this.addPriceToHistory(data.source, data.price, data.timestamp);
            // UI updates will be handled by processMessageQueue
        }
    }

    addPriceToHistory(source, price, timestamp = null) {
        const ts = timestamp ? timestamp / 1000 : Date.now() / 1000;
        
        if (!this.priceHistory.has(source)) {
            this.priceHistory.set(source, []);
        }

        const history = this.priceHistory.get(source);
        const newDataPoint = [ts, price];
        history.push(newDataPoint);

        if (history.length > this.maxHistoryPoints) {
            history.shift();
        }

        // If source is enabled, immediately update the chart series with the new data point
        if (this.isSourceEnabled(source)) {
            const series = this.chartSeries.get(source);
            if (series) {
                const chartDataPoint = {
                    time: ts,
                    value: price
                };
                series.update(chartDataPoint);
                
                // Auto-scroll to realtime for new price updates if user is at realtime position
                if (this.chart && this.isAtRealtime && Date.now() - this.lastUserScrollTime > 3000) {
                    this.chart.timeScale().scrollToRealTime();
                }
            }
        }
    }

    updateSourcePrice(source, price) {
        const previousPrice = this.sources.get(source)?.price || price;
        const change = price - previousPrice;
        const changePercent = previousPrice !== 0 ? (change / previousPrice) * 100 : 0;

        this.sources.set(source, {
            price: price,
            previousPrice: previousPrice,
            change: change,
            changePercent: changePercent,
            lastUpdate: Date.now()
        });

        this.connectedSources.add(source);
    }

    updateSourceList() {
        if (!this.sourceUpdatePending) {
            this.sourceUpdatePending = true;
            setTimeout(() => {
                this.performSourceListUpdate();
                this.sourceUpdatePending = false;
            }, 100);
        }
    }
    
    performSourceListUpdate() {
        const sourceList = document.getElementById('sourceList');
        
        if (this.sources.size === 0) {
            sourceList.innerHTML = '<div class="loading">No data available</div>';
            return;
        }

        // Check if we need to recreate HTML (structure changed) or just update prices
        const existingItems = sourceList.querySelectorAll('.source-item');
        const needsRecreation = existingItems.length !== this.sources.size;
        
        if (needsRecreation) {
            console.log('Recreating source list HTML');
            this.recreateSourceList();
        } else {
            // Just update prices and visual states without recreating HTML
            this.updateSourcePrices();
        }
    }
    
    recreateSourceList() {
        const sourceList = document.getElementById('sourceList');
        const sourceColors = {
            'binance_futures': '#f0b90b',
            'bybit_futures': '#f7931a',
            'hyperliquid_futures': '#97FCE4',
            'kraken_futures': '#5a5aff',
            'okx_futures': '#1890ff',
            'gate_futures': '#6c5ce7',
            'paradex_futures': '#ff6b6b',
            'bingx_futures': '#1E3A8A',
            'binance_spot': '#ffb347',
            'bingx_spot': '#3B82F6',
            'bybit_spot': '#f7931a',
            'pyth': '#00ff88',
        };

        let html = '';
        for (const [source, data] of this.sources.entries()) {
            const changeClass = data.change >= 0 ? 'up' : 'down';
            const changeSymbol = data.change >= 0 ? '↑' : '↓';
            const color = sourceColors[source] || '#888';
            const isEnabled = this.isSourceEnabled(source);
            const opacity = isEnabled ? '1' : '0.4';
            
            html += `
                <div class="source-item" data-source="${source}" style="opacity: ${opacity};">
                    <div style="display: flex; align-items: center; gap: 8px;">
                        <input type="checkbox" id="checkbox-${source}" ${isEnabled ? 'checked' : ''}
                               data-source="${source}"
                               style="margin-right: 4px; cursor: pointer;">
                        <div class="source-color-dot" style="background: ${color};"></div>
                        <div class="source-name">${source.replace('_', ' ')}</div>
                    </div>
                    <div>
                        <span class="source-price">$${this.formatPrice(data.price)}</span>
                        <span class="price-change ${changeClass}">
                            ${changeSymbol} ${Math.abs(data.changePercent).toFixed(3)}%
                        </span>
                    </div>
                </div>
            `;
        }
        
        sourceList.innerHTML = html;
    }
    
    updateSourcePrices() {
        for (const [source, data] of this.sources.entries()) {
            const sourceItem = document.querySelector(`[data-source="${source}"]`);
            if (sourceItem) {
                const priceElement = sourceItem.querySelector('.source-price');
                const changeElement = sourceItem.querySelector('.price-change');
                
                if (priceElement) {
                    priceElement.textContent = `$${this.formatPrice(data.price)}`;
                }
                
                if (changeElement) {
                    const changeClass = data.change >= 0 ? 'up' : 'down';
                    const changeSymbol = data.change >= 0 ? '↑' : '↓';
                    changeElement.className = `price-change ${changeClass}`;
                    changeElement.textContent = `${changeSymbol} ${Math.abs(data.changePercent).toFixed(3)}%`;
                }
            }
        }
    }

    updateChart() {
        if (!this.chartUpdatePending) {
            this.chartUpdatePending = true;
            requestAnimationFrame(() => {
                this.performChartUpdate();
                this.chartUpdatePending = false;
            });
        }
    }

    performChartUpdate() {
        if (!this.chart || this.priceHistory.size === 0) return;

        const sourceNames = ['binance_futures', 'bybit_futures', 'hyperliquid_futures', 'kraken_futures', 'okx_futures', 'gate_futures', 'paradex_futures', 'bingx_futures', 'binance_spot', 'bybit_spot', 'bingx_spot', 'pyth'];
        
        sourceNames.forEach(source => {
            const series = this.chartSeries.get(source);
            if (!series) return;

            if (this.isSourceEnabled(source)) {
                const history = this.priceHistory.get(source) || [];
                if (history.length > 0) {
                    // Convert data to TradingView format: { time: timestamp, value: price }
                    const seriesData = history.map(([timestamp, price]) => ({
                        time: timestamp,
                        value: price
                    }));
                    
                    series.setData(seriesData);
                } else {
                    // Clear data if no history
                    series.setData([]);
                }
            } else {
                // Clear data for disabled sources
                series.setData([]);
            }
        });

        // Only auto-scroll to realtime if user hasn't manually interacted recently
        if (this.isAtRealtime && Date.now() - this.lastUserScrollTime > 3000) {
            this.chart.timeScale().scrollToRealTime();
        }
    }

    setupOpportunitiesTable() {
        // Setup table sorting
        const headers = document.querySelectorAll('.opportunities-table th.sortable');
        headers.forEach(header => {
            header.addEventListener('click', () => {
                const field = header.dataset.sort;
                if (this.currentSort.field === field) {
                    this.currentSort.direction = this.currentSort.direction === 'asc' ? 'desc' : 'asc';
                } else {
                    this.currentSort.field = field;
                    this.currentSort.direction = 'desc';
                }
                this.updateSortHeaders();
                this.updateOpportunitiesTable();
            });
        });
    }

    updateSortHeaders() {
        const headers = document.querySelectorAll('.opportunities-table th.sortable');
        headers.forEach(header => {
            header.classList.remove('sort-asc', 'sort-desc');
            if (header.dataset.sort === this.currentSort.field) {
                header.classList.add(this.currentSort.direction === 'asc' ? 'sort-asc' : 'sort-desc');
            }
        });
    }

    handleArbitrageOpportunity(opportunity) {
        // Add unique ID for tracking
        opportunity.id = Date.now() + Math.random();
        
        this.arbitrageOpportunities.unshift(opportunity);
        
        if (this.arbitrageOpportunities.length > this.maxOpportunities) {
            this.arbitrageOpportunities = this.arbitrageOpportunities.slice(0, this.maxOpportunities);
        }

        this.updateOpportunitiesTable();
    }

    updateOpportunitiesTable() {
        if (!this.opportunitiesUpdatePending) {
            this.opportunitiesUpdatePending = true;
            setTimeout(() => {
                this.performOpportunitiesTableUpdate();
                this.opportunitiesUpdatePending = false;
            }, 250);
        }
    }
    
    performOpportunitiesTableUpdate() {
        const tbody = document.getElementById('opportunitiesTableBody');
        const stats = document.getElementById('opportunitiesStats');
        
        // Filter opportunities by profit and enabled sources
        const filteredOpportunities = this.arbitrageOpportunities.filter(opp =>
            opp.profit_pct >= this.minProfitFilter &&
            this.isSourceEnabled(opp.buy_source) &&
            this.isSourceEnabled(opp.sell_source)
        );

        // Sort opportunities
        const sortedOpportunities = [...filteredOpportunities].sort((a, b) => {
            let aVal = a[this.currentSort.field];
            let bVal = b[this.currentSort.field];
            
            // Handle different data types
            if (this.currentSort.field === 'timestamp') {
                aVal = new Date(aVal);
                bVal = new Date(bVal);
            } else if (typeof aVal === 'string') {
                aVal = aVal.toLowerCase();
                bVal = bVal.toLowerCase();
            }
            
            if (this.currentSort.direction === 'asc') {
                return aVal > bVal ? 1 : -1;
            } else {
                return aVal < bVal ? 1 : -1;
            }
        });

        // Update stats
        stats.textContent = `${filteredOpportunities.length} alerts`;

        if (sortedOpportunities.length === 0) {
            tbody.innerHTML = '<tr><td colspan="7" class="opportunities-empty">No alerts match current filters</td></tr>';
            return;
        }

        // Generate table rows
        let html = '';
        sortedOpportunities.forEach((opp) => {
            const isRecent = Date.now() - opp.timestamp < 5000; // Fresh for 5 seconds
            const profitClass = this.getProfitClass(opp.profit_pct);
            const timeStr = this.formatTime(opp.timestamp);
            
            html += `
                <tr class="${isRecent ? 'fresh' : ''}" data-id="${opp.id}">
                    <td class="symbol-cell">${opp.symbol}</td>
                    <td class="profit-cell ${profitClass}">${opp.profit_pct.toFixed(3)}%</td>
                    <td class="source-cell">${this.formatSourceName(opp.buy_source)}</td>
                    <td class="price-cell">$${this.formatPrice(opp.buy_price)}</td>
                    <td class="source-cell">${this.formatSourceName(opp.sell_source)}</td>
                    <td class="price-cell">$${this.formatPrice(opp.sell_price)}</td>
                    <td class="time-cell">${timeStr}</td>
                </tr>
            `;
        });
        
        tbody.innerHTML = html;
    }

    getProfitClass(profitPct) {
        if (profitPct >= 0.5) return 'high';
        if (profitPct >= 0.2) return 'medium';
        return 'low';
    }

    formatSourceName(source) {
        if (source === 'pyth') {
            return 'PYTH';
        }
        return source.replace('_futures', '').replace('_', ' ').toUpperCase();
    }

    handleSpreadsUpdate(data) {
        if (data.symbol === this.currentSymbol) {
            this.currentSpreads.set(data.symbol, {
                spreads: data.spreads,
                prices: data.prices,
                timestamp: Date.now()
            });
            this.updateSpreadsMatrix();
        }
    }

    updateSpreadsMatrix() {
        if (!this.spreadsUpdatePending) {
            this.spreadsUpdatePending = true;
            setTimeout(() => {
                this.performSpreadsMatrixUpdate();
                this.spreadsUpdatePending = false;
            }, 300);
        }
    }
    
    performSpreadsMatrixUpdate() {
        const matrixContainer = document.getElementById('spreadsMatrix');
        const spreadData = this.currentSpreads.get(this.currentSymbol);
        
        if (!spreadData || !spreadData.spreads) {
            matrixContainer.innerHTML = '<div class="loading">Waiting for price data...</div>';
            return;
        }

        // Get only enabled sources
        const allSources = Object.keys(spreadData.spreads);
        const sources = allSources.filter(source => this.isSourceEnabled(source));
        
        if (sources.length === 0) {
            matrixContainer.innerHTML = '<div class="loading">No enabled sources with data</div>';
            return;
        }

        // Set dynamic grid columns: 1 for row headers + number of sources for data
        matrixContainer.style.gridTemplateColumns = `60px repeat(${sources.length}, 1fr)`;
        
        // Create matrix HTML
        let html = '';
        
        // Header row
        html += '<div class="spread-header"></div>'; // Empty corner
        sources.forEach(sellSource => {
            const shortName = this.getShortSourceName(sellSource);
            html += `<div class="spread-header">${shortName}</div>`;
        });

        // Data rows
        sources.forEach(buySource => {
            const shortBuyName = this.getShortSourceName(buySource);
            html += `<div class="spread-row-header">${shortBuyName}</div>`;
            
            sources.forEach(sellSource => {
                if (buySource === sellSource) {
                    html += '<div class="spread-cell neutral">-</div>';
                } else {
                    const spread = spreadData.spreads[buySource] && spreadData.spreads[buySource][sellSource];
                    if (spread !== undefined) {
                        const spreadClass = this.getSpreadClass(spread);
                        const displaySpread = spread >= 0 ? `+${spread.toFixed(2)}%` : `${spread.toFixed(2)}%`;
                        html += `<div class="spread-cell ${spreadClass}" title="Buy ${this.formatSourceName(buySource)} → Sell ${this.formatSourceName(sellSource)}: ${displaySpread}">${displaySpread}</div>`;
                    } else {
                        html += '<div class="spread-cell neutral">-</div>';
                    }
                }
            });
        });

        matrixContainer.innerHTML = html;
    }

    getShortSourceName(source) {
        const names = {
            'binance_futures': 'BIN-F',
            'bybit_futures': 'BYB-F',
            'hyperliquid_futures': 'HYP',
            'kraken_futures': 'KRK-F',
            'okx_futures': 'OKX-F',
            'gate_futures': 'GAT-F',
            'paradex_futures': 'PDX',
            'bingx_futures': 'BNG-F',
            'binance_spot': 'BIN-S',
            'bingx_spot': 'BNG-S',
            'bybit_spot': 'BYB-S',
            'pyth': 'PYTH'
        };
        return names[source] || source.substring(0, 3).toUpperCase();
    }

    getSpreadClass(spread) {
        if (spread >= this.minProfitFilter) {
            return 'opportunity';
        } else if (spread > 0) {
            return 'positive';
        } else {
            return 'negative';
        }
    }

    formatTime(timestamp) {
        const date = new Date(timestamp);
        const now = new Date();
        const diffMs = now - date;
        
        if (diffMs < 60000) { // Less than 1 minute
            return Math.floor(diffMs / 1000) + 's';
        } else if (diffMs < 3600000) { // Less than 1 hour
            return Math.floor(diffMs / 60000) + 'm';
        } else {
            return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
        }
    }

    changeSymbol(newSymbol) {
        if (newSymbol === this.currentSymbol) return;
        
        this.currentSymbol = newSymbol;
        this.sources.clear();
        this.priceHistory.clear();
        this.arbitrageOpportunities = [];
        
        // Clear all series data
        this.chartSeries.forEach(series => {
            series.setData([]);
        });

        this.updateChartTitle();
        this.updateSourceList();
        this.updateOpportunitiesTable();
        this.currentSpreads.clear();
        this.updateSpreadsMatrix();
        
        document.getElementById('symbolStatus').textContent = newSymbol;
        
        console.log(`Switched to symbol: ${newSymbol}`);
    }

    updateChartTitle() {
        const chartTitle = document.getElementById('chartTitle');
        chartTitle.textContent = `Price Chart - ${this.currentSymbol} (Live)`;
    }


    

}

document.addEventListener('DOMContentLoaded', () => {
    window.scanner = new FuturesArbitrageScanner();
    console.log('Futures Arbitrage Scanner initialized');
});