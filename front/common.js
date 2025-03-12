/**
* ==============================================================================
* Start of ProVe Gadget Script
* ==============================================================================
*/

/**
* ProVe: UI extension for automated PROovenance VErification of knowledge graphs against textual sources
* Developers  : Jongmo Kim (jongmo.kim@kcl.ac.uk), Yiwen Xing (yiwen.xing@kcl.ac.uk), Yihang Zhao (yihang.zhao@kcl.ac.uk), Odinaldo Rodrigues, Albert Merono Penuela, ...
* Inspired by : Recoin: Relative Completeness Indicator (Vevake Balaraman, Simon Razniewski, and Albin Ahmeti), and COOL-WD: COmpleteness toOL for WikiData (Fariz Darari)
*/

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

function calculateTotalCount(data) {
    if (!data) {
        return 0;
    }
    const supportsCount = Object.values(data.SUPPORTS?.result || {}).length;
    const refutesCount = Object.values(data.REFUTES?.result || {}).length;
    const notEnoughInfoCount = Object.values(data['NOT ENOUGH INFO']?.result || {}).length;

    return supportsCount + refutesCount + notEnoughInfoCount;
}

function displayStatementStats(data) {
    // Check if the data object is available
    if (!data) {
        console.error('Data is missing, cannot display statement stats.');
        return;
    }

    // Calculate the statement statistics
    const stats = calculateStatementStats();
    const totalCount = calculateTotalCount(data); // Use the data object here

    console.log('Calculated stats:', stats);

    const $statsContainer = $('<div id="prove-stats"></div>').css({
        'margin-bottom': '10px',
	    'font-weight': 'bold',
	    'padding': '1px 8px',
	    'box-sizing': 'border-box'
    });

    let text = `This item has ${stats.total} statements. ${stats.total - stats.missing} of these statements (${(100 * (1 - stats.missing / stats.total)).toFixed(1)}%) have references, ${totalCount} of which are external.`;

    $statsContainer.text(text);

    if (stats.missing > 0) {
        text += ` The remaining ${stats.missing} statements do not have references and need your support.`;
        
        $statsContainer.text(text);

        const $focusButton = $('<button>Click here to start adding references to those statements</button>').css({
            'margin-left': '10px',
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
            console.log('Focus button clicked');
            const $firstMissing = $('.wikibase-statementview-references-heading .ui-toggler-label:contains("0 references")').first();
            console.log('First missing reference statement found:', $firstMissing.length > 0);

            if ($firstMissing.length) {
                $firstMissing[0].scrollIntoView({ behavior: 'smooth', block: 'center' });
                console.log('Scrolled to first missing reference statement');
                const $addReferenceLink = $firstMissing.closest('.wikibase-statementview').find('.wikibase-statementview-references .wikibase-addtoolbar-container');
                console.log('Add reference link found:', $addReferenceLink.length > 0);

                if ($addReferenceLink.length) {
                    const originalBackgroundColor = $addReferenceLink.css('background-color');
                    const originalTransition = $addReferenceLink.css('transition');

                    console.log('Original background color:', originalBackgroundColor);
                    console.log('Original transition:', originalTransition);

                    $addReferenceLink.css({
                        'background-color': 'yellow',
                        'transition': 'background-color 0.5s ease-in-out'
                    });
                    console.log('Applied yellow background to add reference link');

                    setTimeout(() => {
                        $addReferenceLink.css({
                            'background-color': originalBackgroundColor,
                            'transition': originalTransition
                        });
                        console.log('Restored original styles after 4 seconds');
                    }, 4000);
                } else {
                    console.log('Add reference link not found');
                }
            } else {
                console.log('No statements missing references found');
                alert('No statements missing references found.');
            }
        });

        $statsContainer.append($focusButton);
    }

    return $statsContainer;
}

function updateProveHealthIndicator(data, qid) {
    console.log(data);
    var $indicators = $('div.mw-indicators');
    var healthValue = data.Reference_score;
    const totalStatements = calculateStatementStats().total;

    var $proveContainer = $('<div class="prove-container"></div>').css({
        'display': 'flex',
        'align-items': 'center',
        'margin-left': '10px'
    });

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
        'margin-left': '10px',
        'top': '-5px',
        'cursor': 'pointer',
        'position': 'relative',
        'display': 'inline-flex',
        'align-items': 'center'
    });
    $healthIndicator.append('Reference Score: ' + healthValue + ' ');

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

	const algoVersion = data.algo_version; // Replace with actual version if available elsewhere
	const currentDateTime = data.Requested_time.split('.')[0].replace('T', ' ');
	
	var totalCount = Object.values(data.SUPPORTS.result || {}).length +
	                 Object.values(data.REFUTES.result || {}).length +
	                 Object.values(data['NOT ENOUGH INFO'].result || {}).length;
	
	var supportsCount = Object.values(data.SUPPORTS.result || {}).length;
	var refutesCount = Object.values(data.REFUTES.result || {}).length;
	var notEnoughInfoCount = Object.values(data['NOT ENOUGH INFO'].result || {}).length;
	
	var hoverContent = `
	    ProVe v${algoVersion}<br>
	    Supports: ${supportsCount} (${(supportsCount / totalCount * 100).toFixed(1)}%)<br>
	    Refutes: ${refutesCount} (${(refutesCount / totalCount * 100).toFixed(1)}%)<br>
	    Not Enough Info: ${notEnoughInfoCount} (${(notEnoughInfoCount / totalCount * 100).toFixed(1)}%)<br>
	    ${currentDateTime}
	`;
	
	$healthIndicator.hover(
	    function() {
	        var $hoverBox = $('<div>')
	            .html(hoverContent)
	            .css({
	                position: 'absolute',
	                top: 'calc(100% + 5px)',  // Move it 5px below the indicator
	                left: '50%',
	                transform: 'translateX(-50%)',
	                backgroundColor: 'white',
	                border: '1px solid black',
	                padding: '5px',
	                zIndex: 1000,
	                whiteSpace: 'nowrap',
	                fontSize: '0.9em',
	                boxShadow: '0 2px 5px rgba(0,0,0,0.2)'  // Add a subtle shadow
	            });
	        $(this).append($hoverBox);
	    },
	    function() {
	        $(this).find('div').remove();
	    }
	);
	
	var $proveLink = $('<a>')
	    .attr('href', 'https://www.wikidata.org/wiki/Wikidata:ProVe')
	    .attr('target', '_blank')
	    .attr('title', 'Click to visit Wikidata:ProVe page')
	    .append($healthIndicator);
	
	$proveContainer.append($proveLink);
	$indicators.append($proveContainer);


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
	            console.log('API Response:', responseData);
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
}

function createProveTables(data, $labelsParent) {
    const $container = $('<div id="prove-container"></div>');
    const $statsContainer = displayStatementStats(data).hide();
    const $buttonContainer = $('<div id="prove-buttons"></div>');
    const $toggleButton = $('<button id="prove-toggle">Show/Hide Reference Results</button>');
    const $tablesContainer = $('<div id="prove-tables" style="display: none;"></div>');

    $buttonContainer.append($toggleButton);
    $container.append($buttonContainer).append($statsContainer).append($tablesContainer);

    const categories = [
        { name: "REFUTES", label: "Non-authoritative", color: "#f9e6e6" },
        { name: "NOT ENOUGH INFO", label: "Potentially irrelevant", color: "#fff9e6" },
        { name: "SUPPORTS", label: "Potentially supportive", color: "#e6f3e6" },
        { name: "error", label: "Irretrievable", color: "#e6e6f3" }
    ];

    categories.forEach(category => {
        const $categoryToggle = $(`<button class="prove-category-toggle" data-category="${category.name}" style="display: none;">${category.label}</button>`);
        $categoryToggle.css('background-color', category.color);
        const $table = createTable(category.name, transformData(data[category.name]));
        $table.hide();
       
        $buttonContainer.append($categoryToggle);
        $tablesContainer.append($table);

        $categoryToggle.click(function() {
            $table.slideToggle();
            $(this).toggleClass('active');
        });
    });

    let isProveActive = false;

    $toggleButton.click(function() {
        isProveActive = !isProveActive;
        $(this).toggleClass('active');

        if (isProveActive) {
            $('.prove-category-toggle').show().addClass('active');
            $statsContainer.slideDown();
            $tablesContainer.slideDown();
            $tablesContainer.children().show();
        } else {
            $('.prove-category-toggle').hide().removeClass('active');
            $statsContainer.slideUp();
            $tablesContainer.slideUp(function() {
                $tablesContainer.children().hide();
            });
        }
    });

    $labelsParent.prepend($container);
}

function transformData(categoryData) {
    const result = [];
    const length = categoryData.qid ? Object.keys(categoryData.qid).length : 0;
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
    
    console.log('Transformed data:', result);  
    return result;
}

function createTable(title, data) {
    const colorMap = {
        "SUPPORTS": "#e6f3e6",
        "REFUTES": "#f9e6e6",
        "NOT ENOUGH INFO": "#fff9e6",
        "error": "#e6e6f3"
    };
    const backgroundColor = colorMap[title] || "#f0f0f0";

    const tbheaderMap = {
        "SUPPORTS": "Sentence in external URL found to possibly support the triple",
        "REFUTES": "Sentence in external URL to be checked, possibly not authoritative",
        "NOT ENOUGH INFO": "Sentence in external URL to be checked, possibly not relevant",
        "error": "Irretrievable external sources"
    };

    const resultHeader = tbheaderMap[title] || "ProVe Result Sentences";

    const $table = $(`
        <div class="expandable-table">
            <table data-category="${title}">
                <thead>
                    <tr>
                        <th class="sortable" data-sort="triple">Triple</th>
                        <th class="sortable" data-sort="result_sentence">${resultHeader}</th>
                        <th>URL</th>
                    </tr>
                </thead>
                <tbody>
                </tbody>
            </table>
        </div>
    `);

    const $tbody = $table.find('tbody');

    const addRow = (item) => {
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

        const $row = $(`
            <tr>
                <td><a class="modify-btn">${item.triple}</a></td>
                <td>${resultSentence}</td>
                <td><a href="${item.url}" target="_blank">Link</a></td>
            </tr>
        `);

        $row.find('.modify-btn').click(() => handleModify(item));
        $row.find('.modify-btn').attr('title', 'Click to view triple and edit');

        $tbody.append($row);
    };

    data.forEach(addRow);

    // Add sorting functionality
    $table.find('th.sortable').click(function () {
        const sortBy = $(this).data('sort');
        const sortedData = [...data].sort((a, b) => (a[sortBy] || "").localeCompare(b[sortBy] || ""));
        $tbody.empty();
        sortedData.forEach(addRow);
    });

    return $table;
}

function handleModify(item) {
    console.log('Modifying:', item.pid, item.url);
    
    var pidElement = document.querySelector(`a[title="Property:${item.pid}"]`);
    
    if (!pidElement) {
        console.log(`Element with property ${item.pid} not found`);
        alert(`Property ${item.pid} not found on this page`);
        return;
    }

    pidElement.scrollIntoView({ behavior: 'smooth', block: 'center' });
    console.log('Scrolled to PID element');

    var statementGroupView = pidElement.closest('.wikibase-statementgroupview');
    if (!statementGroupView) {
        console.log('Statement group view not found');
        return;
    }

    var statementViews = statementGroupView.querySelectorAll('.wikibase-statementview');
    
    function processStatements(index) {
        if (index >= statementViews.length) {
            console.log('URL not found in any statement');
            return;
        }

        var statementView = statementViews[index];
        var referencesContainer = statementView.querySelector('.wikibase-statementview-references');
        var referencesHeading = statementView.querySelector('.wikibase-statementview-references-heading');

        if (referencesContainer && referencesHeading && referencesContainer.offsetParent === null) {
            var toggler = referencesHeading.querySelector('.ui-toggler');
            if (toggler) {
                toggler.click();
                console.log('Clicked toggler to expand references');
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
		    console.log('Found the edit link:', previousEditLink);
		} else {
		    console.log('No edit link found.');
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
        
        console.log('Applied highlight style');

        setTimeout(() => {
            urlElement.setAttribute('style', originalStyle);
            console.log('Restored original style');
        }, 3000);
        
        console.log(`Highlighted URL: ${item.url}`);
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
            #prove-buttons {
                display: flex;
                flex-wrap: nowrap;
                margin-bottom: 10px;
                justify-content: space-between; /* Distribute buttons evenly */
                width: 100%; /* Ensure buttons match the width of the table */
                gap: 5px; /* Remove gaps to match the table width */
                box-sizing: border-box; /* Include padding and borders in width calculation */
            }
            #prove-toggle, .prove-category-toggle { 
                flex: 1; /* Make buttons take equal space */
                padding: 10px;
                border: 1px solid #a2a9b1;
                border-radius: 2px;
                cursor: pointer;
                text-align: center;
                height: 50px; /* Fixed height for all buttons */
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
			    display: flex;
			    align-items: center; /* Aligns button and health indicator */
			    gap: 10px; /* Adds space between button and healthIndicator */
			    margin-top: 20px; /* Moves the entire container upwards */
			}
			.health-indicator {
			    margin-top: 5px; /* Moves the health indicator slightly upward */
			    display: inline-flex;
			    align-items: center;
			    position: relative;
			}
			#prove-action-btn {
			    margin-left: 10px;
			    margin-top: -10px; 
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
            .expandable-table th.sortable {
                padding: 15px;
                cursor: pointer;
                position: sticky;
                top: 0;
                background-color: #f8f9fa;
                min-height: 100px;
            }
            .expandable-table th.sortable:hover {
                opacity: 0.8;
            }
            .expandable-table th[data-sort="triple"] {
                width: 30%; /* Fixed width for the 'Triple' column */
            }
            .expandable-table th[data-sort="result_sentence"] {
                width: 60%; /* Fixed width for the 'Result Sentence' column */
            }
            .expandable-table th:last-child {
                width: 10%; /* Fixed width for the 'URL' column */
            }
            .expandable-table td {
                word-break: break-word;
            }
            /* Background colors for categories */
            .expandable-table table[data-category="SUPPORTS"] th {
                background-color: #e6f3e6;
            }
            .expandable-table table[data-category="REFUTES"] th {
                background-color: #f9e6e6;
            }
            .expandable-table table[data-category="NOT ENOUGH INFO"] th {
                background-color: #fff9e6;
            }
            .expandable-table table[data-category="error"] th {
                background-color: #e6e6f3;
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
console.log('prove-plugin loaded');
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
    $indicators.append($proveContainer);
    
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
        console.log('Status API Response:', data);
        const flatdata = Array.isArray(data) ? data[0] : data;
        const status = flatdata.status;
        console.log(status);
        let statusText = '';
        let imageUrl = '';
        // let showPrioritiseButton = false;

        if (status !== 'completed') {
            if (status === 'in queue') {
                statusText = 'ProVe is processing this item';
                imageUrl = 'https://raw.githubusercontent.com/dignityc/prove_for_toolforge/main/pending.png';
            } else if (status === 'error' || status === 'Not processed yet') {
                statusText = 'ProVe has not processed this item yet';
                imageUrl = 'https://raw.githubusercontent.com/dignityc/prove_for_toolforge/main/warning.png';
                // showPrioritiseButton = true;
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
  
   //         // Add button for all statuses
   //         const $button = $('<button id="prove-action-btn"></button>');
   //         const buttonText = (status === 'processing' || status === 'in queue') 
			//     ? 'Fetch Results' 
			//     : (status === 'error' || status === 'Not processed yet') 
			//         ? 'Compute' 
			//         : 'Recompute';
			
			// const hoverText = (status === 'processing' || status === 'in queue') 
			//     ? 'Click to fetch results for this item' 
			//     : (status === 'error' || status === 'Not processed yet') 
			//         ? 'Click to compute ProVe data for this item' 
			//         : 'Click to recompute ProVe data for this item';

   //         $button.text(buttonText).attr('title', hoverText);

   //         $button.click(() => {
   //             const apiUrl = `https://kclwqt.sites.er.kcl.ac.uk/api/requests/requestItem?qid=${entityID}`;

   //             $button.prop('disabled', true).text('Processing...');
                
   //             fetch(apiUrl)
   //                 .then(response => {
   //                     if (!response.ok) {
   //                         throw new Error('Network response was not ok');
   //                     }
   //                     return response.json();
   //                 })
   //                 .then(responseData => {
   //                     console.log('API Response:', responseData);
   //                     const estimatedComputationTimeMinutes = Math.ceil(totalStatements * 7.05 / 60);
   //             		alert(`Your request for recomputation has been successfully queued. The estimated computation time is approximately ${estimatedComputationTimeMinutes} minutes. Please check back later for the updated scores.`);
   //                 })
   //                 .catch(error => {
   //                     console.error('Error:', error);
   //                     alert(`Failed to ${buttonText.toLowerCase()} item. Please try again later.`);
   //                 })
   //                 .finally(() => {
   //                     $button.prop('disabled', false).text(buttonText);
   //                 });
   //         });

   //         //$('div.mw-indicators').append($button);
   //         $proveContainer.append($statusIndicator);
   //         $proveContainer.append($button);
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
			            console.log('API Response:', responseData);
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
                console.log('CompResult API Response:', data);
                updateProveHealthIndicator(data, entityID);
                createProveTables(data, labelsParent);
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
    // init();
    mw.hook( 'wikipage.content' ).add( init );
});

} ( mediaWiki, jQuery) );

/**
* ==============================================================================
* End of ProVe Gadget Script
* ==============================================================================
*/
