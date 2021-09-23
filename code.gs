// Copyright 2021 PolyCraft Digital Inc.

var cc = DataStudioApp.createCommunityConnector();
var DEFAULT_PACKAGE = 'pushengage';

// [START get_authtype]
// https://developers.google.com/datastudio/connector/reference#getauthtype
function getAuthType() {
  var AuthTypes = cc.AuthType;
  return cc
    .newAuthTypeResponse()
    .setAuthType(AuthTypes.KEY)
    .build();
}
// [END get_authtype]

// [START resetAuth]
function resetAuth() {
  var userProperties = PropertiesService.getUserProperties();
  userProperties.deleteProperty('dscc.key');
}
// [END resetAuth]

// [START isauthvalid]
function isAuthValid() {
  var userProperties = PropertiesService.getUserProperties();
  var key = userProperties.getProperty('dscc.key');
  // This assumes you have a validateKey function that can validate
  // if the key is valid.
  function validateKey(key) {
    return false;
  }
}
// [END isauthvalid]

// [START setCredentials]
function setCredentials(request) {
  var key = request.key;

  // Optional
  // Check if the provided key is valid through a call to your service.
  // You would have to have a `checkForValidKey` function defined for
  // this to work.
  var validKey = checkForValidKey(key);
  if (!validKey) {
    return {
      errorCode: 'INVALID_CREDENTIALS'
    };
  }
  var userProperties = PropertiesService.getUserProperties();
  userProperties.setProperty('dscc.key', key);
  return {
    errorCode: 'NONE'
  };
}
// [END setCredentials]

// [START get_config]
// https://developers.google.com/datastudio/connector/reference#getconfig
function getConfig() {
  var config = cc.getConfig();

  config
    .newInfo()
    .setId('instructions')
    .setText('Enter the required parameters for defined date range and data grouping.');

  config.newTextInput()
    .setId('from')
    .setName('Enter the from date')
    .setHelpText('Enter date in format: YYYY-MM-DD')
    .setPlaceholder('YYYY-MM-DD');
  
  config.newTextInput()
    .setId('to')
    .setName('Enter the to date')
    .setHelpText('Enter date in format: YYYY-MM-DD')
    .setPlaceholder('YYYY-MM-DD');
  
  config.newSelectSingle()
    .setId('group_by')
    .setName('The analytics will be fetched by groups as (days, week, month)')
    .setIsDynamic(true)
    .setHelpText('Select how data should group.')
    .addOption(
      config
          .newOptionBuilder()
          .setLabel("days")
          .setValue("days")
    )
    .addOption(
      config
        .newOptionBuilder()
        .setLabel("week")
        .setValue("week")
    )
    .addOption(
      config
        .newOptionBuilder()
        .setLabel("month")
        .setValue("month")
    )
  
  config.setDateRangeRequired(true);
  config.setIsSteppedConfig(false);

  return config.build();
}
// [END get_config]

// [START get_schema]
function getFields() {
  var fields = cc.getFields();
  var types = cc.FieldType;
  var aggregations = cc.AggregationType;

  fields.newDimension()
    .setId('date')
    .setType(types.YEAR_MONTH_DAY);
  
  fields.newMetric()
    .setId('subscriber_count')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
    
  fields.newMetric()
    .setId('desktop_subscriber_count')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
    
  fields.newMetric()
    .setId('mobile_subscriber_count')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
    
  fields.newMetric()
    .setId('unsubscribed_count')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
    
  fields.newMetric()
    .setId('desktop_unsubscribed_count')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
    
  fields.newMetric()
    .setId('mobile_unsubscribed_count')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
    
  fields.newMetric()
    .setId('notification_sent')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
    
  fields.newMetric()
    .setId('net_subscriber_sent')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
    
  fields.newMetric()
    .setId('view_count')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
    
  fields.newMetric()
    .setId('click_count')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
    
  fields.newMetric()
    .setId('ctr')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);

  return fields;
}

// https://developers.google.com/datastudio/connector/reference#getschema
function getSchema(request) {
  return {schema: getFields().build()};
}
// [END get_schema]

// [START get_data]
// https://developers.google.com/datastudio/connector/reference#getdata
function getData(request) {
  request.configParams = validateConfig(request.configParams);

  var requestedFields = getFields().forIds(
    request.fields.map(function(field) {
      return field.name;
    })
  );

  try {
    var apiResponse = fetchDataFromApi(request);
    var normalizedResponse = normalizeResponse(request, apiResponse);
    var data = getFormattedData(normalizedResponse, requestedFields);
  } catch (e) {
    cc.newUserError()
      .setDebugText('Error fetching data from API. Exception details: ' + e)
      .setText(
        'The connector has encountered an unrecoverable error. Please try again later, or file an issue if this error persists.'
      )
      .throwException();
  }

  return {
    schema: requestedFields.build(),
    rows: data
  };
}

/**
 * Gets response for UrlFetchApp.
 *
 * @param {Object} request Data request parameters.
 * @returns {string} Response text for UrlFetchApp.
 */
function fetchDataFromApi(request) {
  var url = [
    'https://api.pushengage.com/apiv1/analytics/summary?from=' +
    request.dateRange.startDate +
    '&to='+
    request.dateRange.endDate,
    '&group_by' +
    request.configParams.group_by
  ].join('');
  var response = UrlFetchApp.fetch(url);
  return response;
}

/**
 * Parses response string into an object. Also standardizes the object structure
 * for single vs multiple packages.
 *
 * @param {Object} request Data request parameters.
 * @param {string} responseString Response from the API.
 * @return {Object} Contains package names as keys and associated download count
 *     information(object) as values.
 */
function normalizeResponse(request, responseString) {
  var response = JSON.parse(responseString);
  var package_list = request.configParams.package.split(',');
  var mapped_response = {};

  if (package_list.length == 1) {
    mapped_response[package_list[0]] = response;
  } else {
    mapped_response = response;
  }

  return mapped_response;
}

/**
 * Formats the parsed response from external data source into correct tabular
 * format and returns only the requestedFields
 *
 * @param {Object} parsedResponse The response string from external data source
 *     parsed into an object in a standard format.
 * @param {Array} requestedFields The fields requested in the getData request.
 * @returns {Array} Array containing rows of data in key-value pairs for each
 *     field.
 */
function getFormattedData(response, requestedFields) {
  var data = [];
  Object.keys(response).map(function(packageName) {
    var package = response[packageName];
    var downloadData = package.downloads;
    var formattedData = downloadData.map(function(dailyDownload) {
      return formatData(requestedFields, packageName, dailyDownload);
    });
    data = data.concat(formattedData);
  });
  return data;
}
// [END get_data]

// https://developers.google.com/datastudio/connector/reference#isadminuser
function isAdminUser() {
  return true;
}

/**
 * Validates config parameters and provides missing values.
 *
 * @param {Object} configParams Config parameters from `request`.
 * @returns {Object} Updated Config parameters.
 */
function validateConfig(configParams) {
  configParams = configParams || {};
  configParams.package = configParams.package || DEFAULT_PACKAGE;

  configParams.package = configParams.package
    .split(',')
    .map(function(x) {
      return x.trim();
    })
    .join(',');

  return configParams;
}

/**
 * Formats a single row of data into the required format.
 *
 * @param {Object} requestedFields Fields requested in the getData request.
 * @param {string} packageName Name of the package who's download data is being
 *    processed.
 * @param {Object} dailyDownload Contains the download data for a certain day.
 * @returns {Object} Contains values for requested fields in predefined format.
 */
function formatData(requestedFields, packageName, dailyDownload) {
  var row = requestedFields.asArray().map(function(requestedField) {
    switch (requestedField.getId()) {
        case 'date':
          return dailyDownload.date.replace(/-/g, '');
        case 'subscriber_count':
          return dailyDownload.subscriber_count;
        case 'desktop_subscriber_count':
          return dailyDownload.desktop_subscriber_count;
        case 'mobile_subscriber_count':
          return dailyDownload.mobile_subscriber_count;
        case 'unsubscribed_count':
          return dailyDownload.unsubscribed_count;
        case 'desktop_unsubscribed_count':
          return dailyDownload.desktop_unsubscribed_count;
        case 'mobile_unsubscribed_count':
          return dailyDownload.mobile_unsubscribed_count;
        case 'notification_sent':
          return dailyDownload.notification_sent;
        case 'net_subscriber_sent':
          return dailyDownload.net_subscriber_sent;
        case 'view_count':
          return dailyDownload.view_count;
        case 'click_count':
          return dailyDownload.click_count;
        case 'ctr':
          return dailyDownload.ctr;
        default:
          return '';
    }
  });
  return {values: row};
}
