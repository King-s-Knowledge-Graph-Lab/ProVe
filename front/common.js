const colorMap = {
    "SUPPORTS": "#e6f3e6",
    "REFUTES": "#f9e6e6",
    "NOT ENOUGH INFO": "#fff9e6",
    "error": "#e6e6f3"
};
const statusMapping = {
    "SUPPORTS": "Supportive",
    "REFUTES": "Refuting",
    "NOT ENOUGH INFO": "Inconclusive",
    "error": "Irretrievable"
};
const sortOrder = {};
const activeFilters = new Set();
var page = 0;
var pageSize = 5;
var currentList = [];

function capitalizeFirstLetter(val) {
    return String(val).charAt(0).toUpperCase() + String(val).slice(1).toLowerCase();
}

function loadentityselector(){
    try {
        $( ".value_input input" ).entityselector( {
                url: 'https://www.wikidata.org/w/api.php',
                language: mw.config.get( 'wgUserLanguage' ),
            } );
    }
    catch(err) {
        setTimeout(loadentityselector, 100);
    }
}

// Prove Functions
function calculateStatementStats() {
    const totalStatements = $('.wikibase-statementview').length;
    const missingReferences = $('.wikibase-statementview-references-heading .ui-toggler-label:contains("0 references")').length;
    return {
        total: totalStatements,
        missing: missingReferences
    };
}

function displayStatementStats(data) {
    // Check if the data object is available
    if (!data) {
        return;
    }

    // Calculate the statement statistics
    const stats = calculateStatementStats();
    let statementsHashmap = new Map();
    for (entry of ["SUPPORTS", "REFUTES", "NOT ENOUGH INFO", "error"]) {
        if (entry in data) {
            new Map(Object.entries(data[entry]?.property_id || {})).forEach((value) => {
                const current = statementsHashmap.get(value) ? statementsHashmap.get(value) : 0;
                statementsHashmap.set(value, 1 + current);
            });
        };
    };
    const externalReferences = statementsHashmap.values().reduce((sum, num) => sum + num, 0);


    const $statsContainer = $('<div id="prove-stats"></div>').css({
        'margin-bottom': '10px',
	    'font-weight': 'bold',
	    'padding': '1px 8px',
	    'box-sizing': 'border-box',
        'display': 'flex'
    });

    let $statsDiv = $(`
        <div>
            This item has ${stats.total} statements. <br>
            <ul>
                <li>${stats.total - stats.missing - statementsHashmap.size} statements have internal references (not checked by ProVe).</li>
                <li>${stats.missing} statements do not have references. <span id="statement-button"></span></li>
                <li>
                    ${statementsHashmap.size} statements have 
                    ${externalReferences} 
                    external references, with the following verification results:
                </li>
            </ul>
        </div>`);

    $statsContainer.append($statsDiv);

    if (stats.missing > 0) {
        const $focusButton = $('<button>Click here to add references to them</button>').css({
            'margin': '0.5rem',
            'padding': '5px 5px',
            'font-size': '12px',
            'font-weight': 'bold',
            'color': 'black',
            'background-color': 'lightgrey',
            'border': 'none',
            'border-radius': '5px',
            'cursor': 'pointer',
            'box-shadow': '0 4px 6px rgba(0, 0, 0, 0.1)'
        }).hover(function() {
            $(this).css({
                'background-color': 'darkgrey',
                'transform': 'scale(1.005)'
            });
        }, function() {
            $(this).css({
                'background-color': 'lightgrey',
                'transform': 'scale(1)'
            });
        }).click(function() {
            const $firstMissing = $('.wikibase-statementview-references-heading .ui-toggler-label:contains("0 references")').first();

            if ($firstMissing.length) {
                $firstMissing[0].scrollIntoView({ behavior: 'smooth', block: 'center' });
                const $addReferenceLink = $firstMissing.closest('.wikibase-statementview').find('.wikibase-statementview-references .wikibase-addtoolbar-container');

                if ($addReferenceLink.length) {
                    const originalBackgroundColor = $addReferenceLink.css('background-color');
                    const originalTransition = $addReferenceLink.css('transition');

                    $addReferenceLink.css({
                        'background-color': 'yellow',
                        'transition': 'background-color 0.5s ease-in-out'
                    });

                    setTimeout(() => {
                        $addReferenceLink.css({
                            'background-color': originalBackgroundColor,
                            'transition': originalTransition
                        });
                    }, 4000);
                } else {
                }
            } else {
                alert('No statements missing references found.');
            }
        });

        $statsDiv.find("#statement-button").append($focusButton);
    }

    return $statsContainer;
}

function updateProveHealthIndicator(data, qid, container) {
    var healthValue = data.Reference_score;
    const totalStatements = calculateStatementStats().total;

    var $proveContainer = $('<div class="prove-health-container"></div>')

    if (typeof healthValue === 'number') {
        healthValue = healthValue.toFixed(2);
    } else if (healthValue === undefined || healthValue === null) {
        healthValue = 'N/A';
    } else if (healthValue === 'processing error') {
        healthValue = 'Not processed yet';
    }

    var imageUrl = '';
    if (healthValue !== 'N/A' && healthValue !== 'Not processed yet') {
        var numericValue = parseFloat(healthValue);
        var imageNumber = 0;
        if (numericValue >= 0.2 && numericValue < 0.4) imageNumber = 1;
        else if (numericValue >= 0.4 && numericValue < 0.6) imageNumber = 2;
        else if (numericValue >= 0.6 && numericValue < 0.8) imageNumber = 3;
        else if (numericValue >= 0.8 && numericValue <= 1) imageNumber = 4;

        imageUrl = `https://raw.githubusercontent.com/dignityc/prove_for_toolforge/main/${imageNumber}.png`;
    }

    var $healthIndicator = $('<span>').css({
        'cursor': 'pointer',
        'position': 'relative',
        'display': 'inline-flex',
        'align-items': 'center'
    });
    $healthIndicator.append('ProVe Score: ' + healthValue + ' ');

    if (imageUrl) {
        var $image = $('<img>')
            .attr('src', imageUrl)
            .css({
                'vertical-align': 'middle',
                'margin-left': '5px',
                'width': '20px',  
                'height': 'auto'  
            });

        $healthIndicator.append($image);
    }

	const algoVersion = data.algo_version;
	let currentDateTime = data.Requested_time.split('.')[0];
    currentDateTime = new Date(currentDateTime);
    currentDateTime = currentDateTime.toLocaleString('en-GB', { 
        year: 'numeric', month: '2-digit', day: '2-digit', 
        hour: '2-digit', minute: '2-digit', second: '2-digit', 
        hour12: false 
    });
	
	var supportsCount = Object.values(data.SUPPORTS.result || {}).length;
	var refutesCount = Object.values(data.REFUTES.result || {}).length;
	var notEnoughInfoCount = Object.values(data['NOT ENOUGH INFO'].result || {}).length;
    var errors = Object.values(data['error'].result || {}).length;
    var totalCount = supportsCount + refutesCount + notEnoughInfoCount + errors
	
	var hoverContent = `
	    ProVe v${algoVersion}<br>
	    Last updated on ${currentDateTime}<br>
	    <span id="hover-non-authoritative" class="hover-item">
            ${statusMapping["REFUTES"]}: ${refutesCount} (${(refutesCount / totalCount * 100).toFixed(1)}%)
        </span>
	    <span id="hover-irrelevant" class="hover-item">
            ${statusMapping["NOT ENOUGH INFO"]}: ${notEnoughInfoCount} (${(notEnoughInfoCount / totalCount * 100).toFixed(1)}%)
        </span>
	    <span id="hover-support" class="hover-item">
            ${statusMapping["SUPPORTS"]}: ${supportsCount} (${(supportsCount / totalCount * 100).toFixed(1)}%)
        </span>
        <span id="hover-irretrievable" class="hover-item">
            ${statusMapping["error"]}: ${errors} (${(errors / totalCount * 100).toFixed(1)}%)
        </span>
	`;
	
    let hoverTimeout; // to let the user hover the hover content without disappearing. 
	$healthIndicator.hover(
	    function() {
            clearTimeout(hoverTimeout);
	        var $hoverBox = $('<div>')
	            .html(hoverContent)
	            .css({
	                position: 'absolute',
	                top: 'calc(100% + 5px)',
	                left: '50%',
	                transform: 'translateX(-50%)',
	                backgroundColor: 'white',
	                border: '1px solid black',
	                padding: '5px',
	                zIndex: 1000,
	                whiteSpace: 'nowrap',
	                fontSize: '0.9em',
	                boxShadow: '0 2px 5px rgba(0,0,0,0.2)'
	            });
	        $(this).append($hoverBox);
	    },
	    function() {
            const self = $(this);
            hoverTimeout = setTimeout(function() {
                self.find('div').remove();
            }, 250);
	    }
	);
	
	var $proveLink = $('<a>')
	    .attr('href', 'https://www.wikidata.org/wiki/Wikidata:ProVe#How_ProVe_Works')
	    .attr('target', '_blank')
	    .attr('title', 'Click to visit Wikidata:ProVe page')
	    .append($healthIndicator);
	
	$proveContainer.append($proveLink);
	container.append($proveContainer);

    // Add button logic
    var $button = $('<button id="prove-action-btn"></button>');
	    
    var buttonText, hoverText;

    if (healthValue === 'Not processed yet' || healthValue === 'processing error') {
        buttonText = 'Compute';
        hoverText = 'Click to compute ProVe data for this item';
    } else {
        buttonText = 'Recompute';
        hoverText = 'Click to recompute ProVe data for this item';
    }
    

    $button.text(buttonText).attr('title', hoverText);

	$button.click(() => {
	    const apiUrl = `https://kclwqt.sites.er.kcl.ac.uk/api/requests/requestItem?qid=${qid}`;
	
	    $button.prop('disabled', true).text('Processing...');
	
	    fetch(apiUrl)
	        .then(response => {
	            if (!response.ok) {
	                throw new Error('Network response was not ok');
	            }
	            return response.json();
	        })
	        .then(responseData => {
	            const estimatedComputationTimeMinutes = Math.ceil(totalStatements * 7.05 / 60);
	
	            let alertMessage;
	            if (buttonText === 'Compute') {
	                alertMessage = `Your request for computation has been successfully queued. The estimated computation time is approximately ${estimatedComputationTimeMinutes} minutes. Please check back later for the updated scores or click fetch results button to check the updated time remaining.`;
	            } else if (buttonText === 'Recompute') {
	                alertMessage = `Your request for recomputation has been successfully queued. The estimated recomputation time is approximately ${estimatedComputationTimeMinutes} minutes. Please check back later for the updated scores or click fetch results button to check the updated time remaining.`;
	            } else if (buttonText === 'Fetch Results') {
	                alertMessage = `Your request for fetch has been successful. The updated estimated computation time is approximately ${estimatedComputationTimeMinutes} minutes. Please check back later for the updated scores or click fetch results button to check the updated time remaining.`;
	            }
	
	            alert(alertMessage);
	        })
	        .catch(error => {
	            console.error('Error:', error);
	            alert(`Failed to ${buttonText.toLowerCase()} item. Please try again later.`);
	        })
	        .finally(() => {
	            $button.prop('disabled', false).text(buttonText);
	        });
	});
	$proveContainer.append($button);
    return $proveContainer;
}

function addRows(data, tbody) {
    tbody.empty();
    const bottom = page * pageSize;
    const upper = (page * pageSize) + pageSize;
    const displayData = data.slice(bottom, upper);
    displayData.forEach((element) => addRow(element, tbody));
}

function createPagination(data, tbody) {
    const $element = $(`
        <div style="display: flex; align-items: center; justify-content: space-between; width: fit-content; margin-top: 0.5rem;">
            <button id="prevButton">Prev</button>
            <span id="pageInfo" style="margin: 0 0.5rem;">
                ${(page + 1)} of ${Math.ceil(data.length / pageSize)}
            </span>
            <button id="nextButton">Next</button>
        </div>
    `)
    
    $element.find("#prevButton").click(function() {
        if (page === 0);
        else  page--;
        document.getElementById("pageInfo").innerText = `${(page + 1)} of ${Math.ceil(data.length / pageSize)}`;
        addRows(currentList, tbody);
    })

    $element.find("#nextButton").click(function() {
        if (page === Math.ceil(data.length / pageSize) - 1);
        else {
            page++;
            var displayNumber = (page + 1);
            if (displayNumber > data.length) displayNumber = data.length;
            document.getElementById("pageInfo").innerText = `${displayNumber} of ${Math.ceil(data.length / pageSize)}`;
            addRows(currentList, tbody);
        }
    });

    return $element
}

function setPageSize(data, tbody) {
    const $pageSizeInput = $(`
        <div>
            <label for="pageSizeSelect">Items per page:</label>
            <select id="pageSizeSelect" style="width: 3rem; font-size: 16px;">
            </select>
        </div>
    `)

    const pageSizes = ["5", "10", "25", "100", "All"];
    pageSizes.forEach(function(element){
        if (parseInt(element) <= data.length) {
            $pageSizeInput.find("#pageSizeSelect").append(`<option value="${element}">${element}</option>`);
        }
        if (element === "All") {
            $pageSizeInput.find("#pageSizeSelect").append(`<option value="${data.length}">All</option>`);
        }
    });

    $pageSizeInput.find("#pageSizeSelect").change(function(element) {
        pageSize = element.target.value;
        page = 0;
        addRows(currentList, tbody);
    });
    return $pageSizeInput

}

function createProveTables(data, container, healthContainer) {
    const $statsContainer = displayStatementStats(data).hide();
    const $buttonContainer = $('<div id="prove-buttons"></div>');
    const $toggleButton = $('<button id="prove-toggle">Show/Hide Reference Results</button>');
    const $filterContainer = $('<div id="prove-filters" style="display: none;"></div>')
    const $tablesContainer = $('<div id="prove-tables" style="display: none;"></div>');
    const $paginationContainer = $('<div id="prove-pagination" style="display: none;"></div>')

    $buttonContainer.append($toggleButton);
    healthContainer.append($buttonContainer);
    container.append($statsContainer).append($filterContainer);
    container.append($paginationContainer).append($tablesContainer);


    const categories = [
        { name: "REFUTES", label: "Refuting", color: "#f9e6e6" },
        { name: "NOT ENOUGH INFO", label: "Inconclusive", color: "#fff9e6" },
        { name: "SUPPORTS", label: "Supportive", color: "#e6f3e6" },
        { name: "error", label: "Irretrievable", color: "#e6e6f3" }
    ];
    
    let filters = '<p> Filters: </p>';
    filters += '<div style="display: flex;">';
    categories.forEach((category) => {
        const categoryStatus = Object.values(data[category.name]?.qid || {}).length > 0;
        filters += `
            <div class="prove-filters-checkbox" data-filter="${category.name}">
                <input type="checkbox" class="checkbox" ${categoryStatus ? "checked" : ""} ${categoryStatus ? "active" : "disabled"}>
                <label for="toggle" class="switch"></label>
                <p class="prove-filters-text ${categoryStatus ? "active" : "disabled"}">${category.label}</p>
            </div>`;
    });
    filters += `</div>`;
    const $checkboxFilter = $(filters);
    $filterContainer.append($checkboxFilter);

    const table = createTable();
    table.hide();
    $tablesContainer.append(table);
    const tbody = table.find('tbody');
    const categoryData = [];

    categories.forEach((category) => {
        const transformedData = transformData(data[category.name]);
        categoryData.push(...transformedData);
        transformedData.forEach((element) => addRow(element, tbody));
    });

    $checkboxFilter.find(".prove-filters-checkbox").click(function() {
        const filterBy = $(this).data('filter');
        const checkbox = $(this).find("input");
        if (checkbox.is(':disabled')) return;
        checkbox.prop('checked', (i, val) => !val);
        if (checkbox.is(":checked")) activeFilters.delete(filterBy);
        else activeFilters.add(filterBy);
        let filteredData = [...currentList].filter((item) => !activeFilters.has(item.result));
        tbody.empty();
        currentList = filteredData;
        addRows(filteredData, tbody);
    });

    table.find('th.sortable').click(function () {
        const sortBy = $(this).data('sort');
        let sortedData = [...categoryData].sort((a, b) => (a[sortBy] || "").localeCompare(b[sortBy] || ""));;
        if (!sortOrder[sortBy]) sortedData = sortedData.reverse();
        sortOrder[sortBy] = !sortOrder[sortBy];
        updateSortArrow(sortOrder[sortBy], sortBy);
        tbody.empty();
        currentList = sortedData;
        addRows(sortedData, tbody);
    });
    table.find('th[data-sort="result_status"]').click();

    $paginationContainer.append(setPageSize(currentList, tbody));
    $paginationContainer.append(createPagination(currentList, tbody));

    let isProveActive = false;

    $toggleButton.click(function() {
        isProveActive = !isProveActive;
        $(this).toggleClass('active');

        if (isProveActive) {
            $('.prove-category-toggle').show().addClass('active');
            $statsContainer.slideDown();
            $filterContainer.slideDown();
            $tablesContainer.slideDown();
            $tablesContainer.children().show();
            $paginationContainer.slideDown();
        } else {
            $('.prove-category-toggle').hide().removeClass('active');
            $statsContainer.slideUp();
            $filterContainer.slideUp();
            $tablesContainer.slideUp(function() {
                $tablesContainer.children().hide();
            });
            $paginationContainer.slideUp();
        }
    });
}

function transformData(categoryData) {
    const result = [];
    const keys = Object.keys(categoryData.qid || {});
    keys.forEach(key => {
        result.push({
            qid: categoryData.qid[key] || 'N/A',
            pid: categoryData.property_id[key] || 'N/A',
            result: categoryData.result[key] || 'N/A',
            result_sentence: categoryData.result_sentence[key] || 'N/A',
            triple: categoryData.triple[key] || 'N/A',
            url: categoryData.url[key] || '#'
        });
    });

    return result;
}

function updateSortArrow(sortOrder, sortBy) {
    const $arrow = $(`th[data-sort="${sortBy}"] .sort-arrow`);
    
    $(`.sort-arrow`).each(function() {
        if (!$(this).hasClass('hidden')) $(this).addClass('hidden');
    });

    if (sortOrder) $arrow.removeClass('rotate-down').removeClass('hidden').addClass('rotate-up');
    else $arrow.removeClass('rotate-up').removeClass('hidden').addClass('rotate-down');
}

function createTable() {
    const $table = $(`
        <div class="expandable-table">
            <table>
                <thead>
                    <tr>
                        <th class="sortable" data-sort="triple">
                            Statements
                            <span class="sort-arrow hidden"></span>
                        </th>
                        <th class="sortable" data-sort="result_sentence">
                            Relevant sentence in reference/Status code
                            <span class="sort-arrow hidden"></span>
                        </th>
                        <th class="sortable" data-sort="result_status">
                            Support stance
                            <span class="sort-arrow hidden"></span>
                        </th>
                        <th  class="sortable" data-sort="reference">
                            Reference
                            <span class="sort-arrow hidden"></span>
                        </th>
                    </tr>
                </thead>
                <tbody>
                </tbody>
            </table>
        </div>
    `);


    return $table;
}

function addRow(item, tbody) {
    // Adjust result_sentence for errors if needed
    let resultSentence = item.result_sentence;
    if (resultSentence === "Error: HTTP status code 403") {
        resultSentence = "The document linked by this URL is restricted and cannot be accessed (Error 403: Access Denied)";
    } else if (resultSentence === "Error: HTTP status code 404") {
        resultSentence = "The document linked by this URL could not be found (Error 404: Not Found)";
    }
    
    // Remove "Source language: (xx) / " or "Source language: (None) / " at the beginning
    const slashIndex = resultSentence.indexOf('/');
    if (slashIndex !== -1) {
        resultSentence = resultSentence.substring(slashIndex + 1).trim();
    }

    const label = document.getElementsByClassName('wikibase-title-label')[0].innerText
    const name = item.triple.startsWith(label) ? item.triple.substring(label.length + 1) : item.triple;
    const $row = $(`
        <tr class="row-${item.result.toLowerCase().replace(/ /g, '-')}">
            <td><a style="white-space: normal;" class="modify-btn">${name}</a></td>
            <td><div class="scroll-content">${resultSentence}</div></td>
            <td>${capitalizeFirstLetter(statusMapping[item.result])}</td>
            <td><a href="${item.url}" target="_blank" title="${item.url}">${item.url}</a></td>
        </tr>
    `);

    $row.find('.modify-btn').click(() => handleModify(item));
    $row.find('.modify-btn').attr('title', 'Click to view triple and edit');
    tbody.append($row);
}

function handleModify(item) {    
    var pidElement = document.querySelector(`a[title="Property:${item.pid}"]`);
    
    if (!pidElement) {
        alert(`Property ${item.pid} not found on this page`);
        return;
    }

    pidElement.scrollIntoView({ behavior: 'smooth', block: 'center' });

    var statementGroupView = pidElement.closest('.wikibase-statementgroupview');
    if (!statementGroupView) {
        return;
    }

    var statementViews = statementGroupView.querySelectorAll('.wikibase-statementview');
    
    function processStatements(index) {
        if (index >= statementViews.length) {
            return;
        }

        var statementView = statementViews[index];
        var referencesContainer = statementView.querySelector('.wikibase-statementview-references');
        var referencesHeading = statementView.querySelector('.wikibase-statementview-references-heading');

        if (referencesContainer && referencesHeading && referencesContainer.offsetParent === null) {
            var toggler = referencesHeading.querySelector('.ui-toggler');
            if (toggler) {
                toggler.click();
                setTimeout(() => checkForUrl(statementView, index), 300);
            } else {
                checkForUrl(statementView, index);
            }
        } else {
            checkForUrl(statementView, index);
        }
    }

    function checkForUrl(statementView, index) {
        var urlElement = statementView.querySelector(`a[href="${item.url}"]`);
        var previousEditLink = urlElement.closest('.wikibase-statementview').querySelector('.wikibase-edittoolbar-container');
		
		if (previousEditLink) {
		    highlightUrl(previousEditLink);
		} else {
		}
        
        if (urlElement) {
            highlightUrl(urlElement);
            
        } else {
            processStatements(index + 1);
        }
    }

    function highlightUrl(urlElement) {
        var originalStyle = urlElement.getAttribute('style') || '';
        
        urlElement.style.backgroundColor = 'yellow';
        urlElement.style.padding = '2px';
        urlElement.style.border = '1px solid #000';
        
        urlElement.scrollIntoView({ behavior: 'smooth', block: 'center' });

        setTimeout(() => {
            urlElement.setAttribute('style', originalStyle);
        }, 3000);
    }

    processStatements(0);
}

function addStyles() {
    $('<style>')
        .prop('type', 'text/css')
        .html(`
            #prove-container {
                margin-bottom: 20px;
                display: flex;
                flex-direction: column;
                align-items: stretch; /* Ensure child elements match container width */
                width: 100%; /* Matches the width of the table */
            }
            #prove-filters {
                display: flex;
                align-items: center;
            }
            .prove-filters-checkbox {
                display: flex;
                align-items: center;
                margin-left: 0.5rem;
                cursor: pointer;
            }
            .prove-filters-text {
                margin-left: 0.25rem !important;
            }
            .disabled {
                color: #C6C6C6;
            }
            #prove-buttons {
                display: flex;
                flex-wrap: nowrap;
                margin-bottom: 0.5rem;
                margin-top: 0.5rem;
                justify-content: space-between; /* Distribute buttons evenly */
                width: fit-content; /* Ensure buttons match the width of the table */
                gap: 5px; /* Remove gaps to match the table width */
                box-sizing: border-box; /* Include padding and borders in width calculation */
            }
            #prove-toggle, .prove-category-toggle { 
                flex: 1; /* Make buttons take equal space */
                padding: 5px 10px;
                border: 1px solid #a2a9b1;
                font-size: 14px;
                border-radius: 5px;
                cursor: pointer;
                text-align: center;
                background-color: #f8f9fa;
                box-sizing: border-box; /* Include padding and borders in the width */
            }
            #prove-toggle:hover, .prove-category-toggle:hover {
                opacity: 0.8;
            }
            #prove-toggle.active, .prove-category-toggle.active {
                font-weight: bold;
                box-shadow: inset 0 0 5px rgba(0,0,0,0.2);
            }
            #prove-stats {
                width: 100%; /* Match the width of the table */
                background-color: #f8f9fa;
                padding: 10px 15px;
                border: 1px solid #a2a9b1;
                border-radius: 2px;
                margin-bottom: 10px;
                box-sizing: border-box; /* Include padding and borders in width calculation */
            }
			.prove-container {
                margin-bottom: 1rem;
			}
            .prove-health-container {
                display: flex;
                align-items: center;
            }
			.health-indicator {
			    margin-top: 5px; /* Moves the health indicator slightly upward */
			    display: inline-flex;
			    align-items: center;
			    position: relative;
			}
			#prove-action-btn {
			    margin-left: 0.5rem;
			    margin-right: 0.5rem;
			    padding: 5px 10px;
			    border: 1px solid #a2a9b1;
			    border-radius: 5px;
			    cursor: pointer;
			    font-size: 14px;
			    background-color: #f8f9fa;
			    box-shadow: 0px 2px 4px rgba(0, 0, 0, 0.1);
			    transition: background-color 0.3s ease; /* Smooth hover effect */
			}
			
			#prove-action-btn:hover {
			    background-color: #e0e0e0; /* Subtle hover effect */
			}
            .expandable-table {
                width: 100%;
                max-width: 100%; /* Prevents table overflow */
                border: 1px solid #c8c8c8;
                margin-top: 5px;
                overflow-y: auto;
            }
            .expandable-table table {
                width: 100%;
                border-collapse: collapse;
                table-layout: fixed; /* Ensures consistent column widths */
            }
            .expandable-table th, .expandable-table td {
                border: 1px solid #c8c8c8;
                text-align: left;
                vertical-align: top;
            }
            .expandable-table th {
                background-color: #f8f9fa;
                cursor: pointer;
            }
            .expandable-table th.sortable:hover {
                opacity: 0.8;
            }
            .expandable-table th[data-sort="triple"] {
                width: 20%; /* Fixed width for the 'Triple' column */
            }
            .expandable-table th[data-sort="result_sentence"] {
            }
            .expandable-table th[data-sort="result_status"] {
                width: 12%; /* Fixed width for the 'Result Sentence' column */
            }
            .expandable-table th:last-child {
                width: 20%; /* Fixed width for the 'URL' column */
            }
            .expandable-table td {
                word-break: break-word;
            }
            .expandable-table td a {
                width: 100%;
                display: inline-block;
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
                vertical-align: middle;
            }
            .sort-arrow {
                vertical-align: middle;
                display: inline-block;
                width: 0;
                height: 0;
                border-left: 5px solid transparent;
                border-right: 5px solid transparent;
                border-top: 5px solid black;
                margin-left: 5px;
                transition: transform 0.3s ease;
            }
            #prove-pagination {
                display: flex;
                justify-content: space-between;
                align-items: center;
            }

            /* Rotate the arrow for descending (downward) sort order */
            .rotate-down {
                transform: rotate(180deg);
            }

            /* Rotate the arrow for ascending (upward) sort order */
            .rotate-up {
                transform: rotate(0deg);
            }
            /* Background colors for categories */
            .expandable-table table[data-category="SUPPORTS"] th {
                background-color: ${colorMap["SUPPORTS"]};
            }
            .expandable-table table[data-category="REFUTES"] th {
                background-color: ${colorMap["REFUTES"]};
            }
            .expandable-table table[data-category="NOT ENOUGH INFO"] th {
                background-color: ${colorMap["NOT ENOUGH INFO"]};
            }
            .expandable-table table[data-category="error"] th {
                background-color: ${colorMap["error"]};
            }
            .row-supports {
                background-color: ${colorMap["SUPPORTS"]};
            }
            .row-refutes {
                background-color: ${colorMap["REFUTES"]};
            }
            .row-not-enough-info {
                background-color: ${colorMap["NOT ENOUGH INFO"]};
            }
            .row-error {
                background-color: ${colorMap["error"]};
            }

            .switch { 
                cursor: pointer;
                position : relative ;
                display : inline-block;
                width : 2rem;
                height : 1rem;
                background-color: #eee;
                border-radius: 20px;
            }
            .switch::after {
                content: '';
                position: absolute;
                width: 0.95rem;
                height: 0.95rem;
                border-radius: 50%;
                background-color: white;
                transition: all 0.3s;
            }
            .scroll-content {
                max-height: 10rem;
                overflow-y: auto;
                display: flex;
            }
            .checkbox:checked + .switch::after {
                right: 0; 
            }
            .checkbox:checked + .switch {
                background-color: #7983ff;
            }
            .checkbox { 
                display : none;
            }


            .hidden {
                visibility: hidden;
            }
            .hover-item {
                width: 100%;
                display: flex;
            }
            .hover-item:before {
                content: "\\00a0";
            }
            #hover-support {
                background-color: ${colorMap["SUPPORTS"]};
            }
            #hover-non-authoritative {
                background-color: ${colorMap["REFUTES"]};
            }
            #hover-irrelevant {
                background-color: ${colorMap["NOT ENOUGH INFO"]};
            }
            #hover-irretrievable {
                background-color: ${colorMap["error"]};
            }
            /* Media Query for Smaller Screens */
            @media (max-width: 1000px) {
                #prove-buttons {
                    flex-direction: column; /* Stack buttons vertically */
                    align-items: stretch; /* Ensure buttons stretch to full container width */
                    width: 100%;
                }
                #prove-toggle, .prove-category-toggle {
                    width: 100%; /* Make each button span the entire row */
                    margin-bottom: 5px; /* Add space between buttons */
                }
            }
        `)
        .appendTo('head');
}

//Initiate the plugin
( 
function( mw, $ ) {
    'use strict';
    /**
     * Check if we're viewing an item
     */
    var entityID = mw.config.get( 'wbEntityId' );
    var lang = mw.config.get( 'wgUserLanguage' );
    var pageid = "48139757";

    if ( !entityID ) 
    {
        return;
    }
    /**
     * holds the DOM input element for the label
     */
    var labelsParent;

    function init() 
    {
        const totalStatements = calculateStatementStats().total;

        // Element into which to add the missing attributes
        var labelsParent = $('#wb-item-' + entityID + ' div.wikibase-entitytermsview-heading');
        if (labelsParent.length < 1) 
        {
            return;
        }
        var $link = $( '<a href="https://www.wikidata.org/wiki/Wikidata:ProVe">' );
        var $img = $( '<img>' ).css('margin-bottom', '15px');
        $link.append( $img ).prependTo( 'div.mw-indicators' );
    
        var $indicators = $('div.mw-indicators');
        var $proveContainer = $('<div class="prove-container"></div>');
        
        addStyles();
        
        // Check item api status
        fetch(`https://kclwqt.sites.er.kcl.ac.uk//api/items/checkItemStatus?qid=${entityID}`)
        .then(response => {
            if (!response.ok) {
                throw new Error('Network response was not ok');
            }
            return response.json();
        })
        .then(data => {
            const flatdata = Array.isArray(data) ? data[0] : data;
            const status = flatdata.status;
            let statusText = '';
            let imageUrl = '';

            if (status !== 'completed') {
                if (status === 'in queue') {
                    statusText = 'ProVe is processing this item';
                    imageUrl = 'https://raw.githubusercontent.com/dignityc/prove_for_toolforge/main/pending.png';
                } else if (status === 'error' || status === 'Not processed yet') {
                    statusText = 'ProVe has not processed this item yet';
                    imageUrl = 'https://raw.githubusercontent.com/dignityc/prove_for_toolforge/main/warning.png';
                } else {
                    statusText = 'Status: ' + status;
                    imageUrl = 'https://raw.githubusercontent.com/dignityc/prove_for_toolforge/main/warning.png';
                }
                
                // Create status indicator
                var $statusIndicator = $('<a>')
                    .attr('href', 'https://www.wikidata.org/wiki/Wikidata:ProVe')
                    .attr('target', '_blank')
                    .css({
                        'margin-top': '10px',
                        'margin-left': '10px',
                        'cursor': 'pointer',
                        'position': 'relative',
                        'display': 'inline-flex',
                        'align-items': 'center',
                        'text-decoration': 'none',
                        'color': 'inherit'
                    });
                
                // Add ProVe text
                $statusIndicator.append($('<span>').text('ProVe'));
                
                // Add image if available
                if (imageUrl) {
                    var $image = $('<img>')
                        .attr('src', imageUrl)
                        .css({
                            'vertical-align': 'middle',
                            'margin-left': '5px',
                            'width': '20px',  
                            'height': 'auto'  
                        });
                    
                    $statusIndicator.append($image);
                }
                
                // Add hover functionality
                $statusIndicator.hover(
                    function() {
                        var $hoverBox = $('<div>')
                            .text(statusText)
                            .css({
                                position: 'absolute',
                                top: 'calc(100% + 5px)',
                                left: '50%',
                                transform: 'translateX(-50%)',
                                backgroundColor: 'white',
                                border: '1px solid black',
                                padding: '5px',
                                zIndex: 1000,
                                whiteSpace: 'nowrap',
                                fontSize: '0.9em',
                                boxShadow: '0 2px 5px rgba(0,0,0,0.2)'
                            });
                        $(this).append($hoverBox);
                    },
                    function() {
                        $(this).find('div').remove();
                    }
                );
                
                // Add status text to labelsParent
                labelsParent.prepend($('<span>').text(statusText).css('margin-right', '10px'));
                
                // Append the combined element to mw-indicators
                $('div.mw-indicators').append($statusIndicator);

                // Add button for all statuses
                const $button = $('<button id="prove-action-btn"></button>');
                const buttonText = (status === 'processing' || status === 'in queue') 
                    ? 'Fetch Results' 
                    : (status === 'error' || status === 'Not processed yet') 
                        ? 'Compute' 
                        : 'Recompute';
                
                const hoverText = (status === 'processing' || status === 'in queue') 
                    ? 'Click to fetch results for this item' 
                    : (status === 'error' || status === 'Not processed yet') 
                        ? 'Click to compute ProVe data for this item' 
                        : 'Click to recompute ProVe data for this item';
                
                $button.text(buttonText).attr('title', hoverText);
                
                $button.click(() => {
                    const apiUrl = `https://kclwqt.sites.er.kcl.ac.uk/api/requests/requestItem?qid=${entityID}`;
                
                    $button.prop('disabled', true).text('Processing...');
                    
                    fetch(apiUrl)
                        .then(response => {
                            if (!response.ok) {
                                throw new Error('Network response was not ok');
                            }
                            return response.json();
                        })
                        .then(responseData => {
                            const estimatedComputationTimeMinutes = Math.ceil(totalStatements * 7.05 / 60);
                
                            // Custom alert messages for different button actions
                            let alertMessage;
                            if (buttonText === 'Compute') {
                                alertMessage = `Your request for computation has been successfully queued. The estimated computation time is approximately ${estimatedComputationTimeMinutes} minutes. Please check back later for the updated scores or click the Fetch Results button to check the updated time remaining.`;
                            } else if (buttonText === 'Recompute') {
                                alertMessage = `Your request for recomputation has been successfully queued. The estimated recomputation time is approximately ${estimatedComputationTimeMinutes} minutes. Please check back later for the updated scores or click the Fetch Results button to check the updated time remaining.`;
                            } else if (buttonText === 'Fetch Results') {
                                alertMessage = `Your request for fetch has been successful. The updated estimated computation time is approximately ${estimatedComputationTimeMinutes} minutes. Please check back later for the updated scores or click the Fetch Results button to check the updated time remaining.`;
                            }
                
                            alert(alertMessage);
                        })
                        .catch(error => {
                            console.error('Error:', error);
                            alert(`Failed to ${buttonText.toLowerCase()} item. Please try again later.`);
                        })
                        .finally(() => {
                            $button.prop('disabled', false).text(buttonText);
                        });
                });
                
                // Append status indicator and button to container
                $proveContainer.append($statusIndicator);
                $proveContainer.append($button);
                $indicators.append($proveContainer);

            } else {
                // If status is complete, fetch ProVe data and initialize main functionality
                fetch(`https://kclwqt.sites.er.kcl.ac.uk/api/items/getCompResult?qid=${entityID}`)
                .then(response => {
                    if (!response.ok) {
                        throw new Error('Network response was not ok');
                    }
                    return response.json();
                })
                .then(data => {
                    const $container = $('<div id="prove-container"></div>');
                    let healthContainer = updateProveHealthIndicator(data, entityID, $container);
                    createProveTables(data, $container, healthContainer);
                    labelsParent.append($container);
                })
                .catch(error => {
                    console.error('Error fetching CompResult:', error);
                    alert('An error occurred while fetching ProVe data. Please try again later.');
                });
            }
        })
        .catch(error => {
            console.error('Error fetching item status:', error);
            var $errorIndicator = $('<span>').text('Error checking ProVe status').css({
                'margin-left': '10px',
                'cursor': 'default',
                'color': 'red'
            });
            $('div.mw-indicators').append($errorIndicator);
        });
    }


    $( function () {
        mw.hook( 'wikipage.content' ).add( init );
    });

} ( mediaWiki, jQuery) );

/**
* ==============================================================================
* End of ProVe Gadget Script
* ==============================================================================
*/
