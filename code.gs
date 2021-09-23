var cc = DataStudioApp.createCommunityConnector();

/**
 * Mandatory function required by Google Data Studio that should
 * return the authentication method required by the connector
 * to authorize the third-party service.
 * @return {Object} AuthType
 */

function getAuthType() {
  return cc.newAuthTypeResponse()
  .setAuthType(cc.AuthType.KEY)
  .setHelpUrl('https://www.pushengage.com/api/')
  .build();
}

/**
 * Mandatory function required by Google Data Studio that should
 * clear user credentials for the third-party service.
 * This function does not accept any arguments and
 * the response is empty.
 */

function resetAuth() {
  var userProperties = PropertiesService.getUserProperties();
  userProperties.deleteProperty('dscc.key');
}

/**
 * Mandatory function required by Google Data Studio that should
 * determine if the authentication for the third-party service is valid.
 * @return {Boolean}
 */

function isAuthValid() {
  var userProperties = PropertiesService.getUserProperties();
  var key = userProperties.getProperty('dscc.key');
  return checkForValidKey(key);
}

/**
 * Mandatory function required by Google Data Studio that should
 * set the credentials after the user enters either their
 * credential information on the community connector configuration page.
 * @param {Object} request The set credentials request.
 * @return {object} An object with an errorCode.
 */

function setCredentials(request) {
  var key = request.key;
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

/**
 * Mandatory function required by Google Data Studio that should
 * return the user configurable options for the connector.
 * @param {Object} request
 * @return {Object} fields
 */

function getConfig(request) {
  var config = cc.getConfig();
  
  config.newInfo()
    .setId('instructions')
    .setText('Enter the required parameters for defined date range and data grouping.');
  
  config.newTextInput()
    .setId('from')
    .setName('Enter the from date')
    .setHelpText('Enter date in format: YYYY-MM-DD')
    .setPlaceholder('YYYY-MM-DD');
  
  config.setDateRangeRequired(true);
  config.newTextInput()
    .setId('to')
    .setName('Enter the to date')
    .setHelpText('Enter date in format: YYYY-MM-DD')
    .setPlaceholder('YYYY-MM-DD');
  
  config.setDateRangeRequired(true);
  config.newSELECT_SINGLE()
    .setId('group_by')
    .setName('The analytics will be fetched by groups as (days, week, month)')
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
  
  config.setAllowOverride(true);
  
  return config.build();
}

/**
 * Supports the getSchema() function
 * @param {Object} request
 * @return {Object} fields
 */

function getFields(request) {
  var cc = DataStudioApp.createCommunityConnector();
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

/**
 * Mandatory function required by Google Data Studio that should
 * return the schema for the given request.
 * This provides the information about how the connector's data is organized.
 * @param {Object} request
 * @return {Object} fields
 */

function getSchema(request) {
  var fields = getFields(request).build();
  return { schema: fields };
}

/**
 * Takes the requested fields with the API response and
 * return rows formatted for Google Data Studio.
 * @param {Object} requestedFields
 * @param {Object} response
 * @return {Array} values
 */

function responseToRows(requestedFields, response, packageName) {
  // Transform parsed data and filter for requested fields
  return response.map(function(dailyDownload) {
    var row = [];
    requestedFields.asArray().forEach(function (field) {
      switch (field.getId()) {
        case 'date':
          return row.push(dailyDownload.date.replace(/-/g, ''));
        case 'subscriber_count':
          return row.push(dailyDownload.subscriber_count);
        case 'desktop_subscriber_count':
          return row.push(dailyDownload.desktop_subscriber_count);
        case 'mobile_subscriber_count':
          return row.push(dailyDownload.mobile_subscriber_count);
        case 'unsubscribed_count':
          return row.push(dailyDownload.unsubscribed_count);
        case 'desktop_unsubscribed_count':
          return row.push(dailyDownload.desktop_unsubscribed_count);
        case 'mobile_unsubscribed_count':
          return row.push(dailyDownload.mobile_unsubscribed_count);
        case 'notification_sent':
          return row.push(dailyDownload.notification_sent);
        case 'net_subscriber_sent':
          return row.push(dailyDownload.net_subscriber_sent);
        case 'view_count':
          return row.push(dailyDownload.view_count);
        case 'click_count':
          return row.push(dailyDownload.click_count);
        case 'ctr':
          return row.push(dailyDownload.ctr);
        default:
          return row.push('');
      }
    });
    return { values: row };
  });
}

/**
* Mandatory function required by Google Data Studio that should
* return the tabular data for the given request.
* @param {Object} request
* @return {Object}
*/

function getData(request) {
  var requestedFieldIds = request.fields.map(function(field) {
    return field.name;
  });
  var requestedFields = getFields().forIds(requestedFieldIds);
  var userProperties = PropertiesService.getUserProperties();
  var token = userProperties.getProperty('dscc.key');
  // Fetch and parse data from API
  var url = [
    'https://api.pushengage.com/apiv1/analytics/summary?from=' +
    request.dateRange.startDate +
    '&to='+
    request.dateRange.endDate,
    '&group_by=' +
    request.configParams.group_by
  ];
  var options = {
    'method' : 'GET',
    'headers': {
      'Authorization': token,
      'Content-Type': 'application/json'
    },
    'muteHttpExceptions':true
  };
  var response = UrlFetchApp.fetch(url, options);
  if (response.getResponseCode() == 200) {
    var parsedResponse = JSON.parse(response);
    var rows = responseToRows(requestedFields, parsedResponse);
    return {
      schema: requestedFields.build(),
      rows: rows
    };
  } else {
    DataStudioApp.createCommunityConnector()
    .newUserError()
    .setDebugText('Error fetching data from API. Exception details: ' + response)
    .setText('Error fetching data from API. Exception details: ' + response)
    .throwException();
  }
}
  /**
  * Checks if the Key/Token provided by the user is valid
  * @param {String} key
  * @return {Boolean}
  */
  function checkForValidKey(key) {
    var token = key;
    var today = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd');
    var url = 'https://api.pushengage.com/apiv1/analytics/summary?from=' + today + '&to=' + today + '&group_by=day';  
    var options = {
      'method' : 'GET',
      'headers': {
        'Authorization': token,
        'Content-Type': 'application/json'
      },
      'muteHttpExceptions':true
    };
    var response = UrlFetchApp.fetch(url, options);
    if (response.getResponseCode() == 200) {
      return true;
    } else {
      return false;
    }
  }
