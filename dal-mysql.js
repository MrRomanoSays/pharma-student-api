const mysql = require('mysql');
const moment = require('moment')

const {
    map,
    filter,
    uniq,
    prop,
    omit,
    compose,
    drop,
    path,
    pathOr,
    view,
    lensIndex,
    set,
    lensPath,
    toString,
    lensProp
} = require('ramda')


var dal = {
    getPatient: getPatient,
    getPatients: getPatients,
    getMed: getMed,
    listMedsByLabel: listMedsByLabel,
    getPharmacy: getPharmacy
}

module.exports = dal

/////////////////////
//   medications
/////////////////////
function getMed(id, callback) {
    getDocByID('medWithIngredients', id, formatMed, function(err, res) {
        if (err) return callback(err)
        //callback(null, view(lensIndex(0), res))
        callback(null, res)
    })
}


function getMed(medId, cb) {
    // getDocByID('medWithIngredients', id, formatMed, function(err, res) {
    //     if (err) return callback(err)
    //     //callback(null, view(lensIndex(0), res))
    //     callback(null, res)
    // })
    if (!medId) return cb({
        error: 'missing_id',
        reason: 'missing_id',
        name: 'missing_id',
        status: 400,
        message: 'unable to retrieve medication due to missing id.'
    })

    const connection = createConnection()

    connection.query('SELECT * FROM medWithIngredients WHERE ID = ?', [medId], function(err, medRows) {
        if (err) return cb({
            error: 'unknown',
            reason: 'unknown',
            name: 'unknown',
            status: 500,
            message: err.message
        })
        if (medRows.length === 0) return cb({
            error: 'not_found',
            reason: 'missing',
            name: 'not_found',
            status: 404,
            message: 'You have attempted to retrieve a medication that is not in the database or has been deleted.'
        })

        cb(null, formatSingleMed(medRows))
    })
}

function formatMultipleMeds(meds) {

    const uniqueIds = compose (
      uniq(),
      map(med=>med.ID))(meds)

    return map(id=>compose(
        formatSingleMed,
        filter(med=>med.ID === id)
    )(meds))(uniqueIds)
}

//////////////////////////////////////////////////////
/////// HELPER FUNCTIONS FOR FORMATTING MEDS
//////////////////////////////////////////////////////

function formatSingleMed(medRows) {

  const mappedIngredients = compose(
      map(med => med.ingredient),
      filter(med => med.ingredient)
  )(medRows)

  return compose(
    omit(['ID', 'ingredient']),
    set(lensProp('ingredients'), mappedIngredients),
    set(lensProp('_id'), toString(prop('ID', medRows[0]))),
    set(lensProp('_rev'), ""),
    set(lensProp('type'), "medication")
  )(medRows[0])

}


function listMedsByLabel (startKey, limit, cb) {
  const connection = createConnection()
  //PAGINATION, default limit of 5...
  limit = limit ? limit : 5
  const whereClause = startKey ? " WHERE concat(m.label, m.ID) > '" + startKey + "'" : ""

  let sql = 'SELECT m.*, concat(m.label, m.ID) as startKey '
  sql += ' FROM medWithIngredients m '
  sql += ' INNER JOIN (SELECT DISTINCT ID '
  sql += ' FROM medWithIngredients m'
  sql += whereClause
  sql += ' LIMIT ' + limit + ') b '
  sql += ' ON m.ID = b.ID '
  sql += whereClause
  sql += ' ORDER BY startKey'

  // console.log("sql: ", sql)

  connection.query(sql, function(err, data) {
    if(err) return cb({
      error: 'unknown',
      reason: 'unknown',
      name: 'unknown',
      status: 500,
      message: err.message
    })
    if (data.length === 0) return cb({
      error: 'not_found',
      reason: 'missing',
      name: 'not_found',
      status: '404',
      message: 'You have attempted to retreive medications that are not in the database.'
    })
    cb(null, formatMultipleMeds(data))
  })
}


/////////////////////
// PATIENTS
/////////////////////

function getPatient(patientId, cb) {

  if (!patientId) return cb({
      error: 'missing_id',
      reason: 'missing_id',
      name: 'missing_id',
      status: 400,
      message: 'unable to retrieve data due to missing id.'
  })

  const connection = createConnection()

  connection.query('SELECT * FROM patientWithConditions WHERE ID = ?', [patientId], function(err, data) {
      if (err) return cb({
          error: 'unknown',
          reason: 'unknown',
          name: 'unknown',
          status: 500,
          message: err.message
      })
      if (data.length === 0) return cb({
          error: 'not_found',
          reason: 'missing',
          name: 'not_found',
          status: 404,
          message: 'missing'
      })


      cb(null, formatSinglePatient(data))
  })
}

function getPatients(startKey, limit, cb) {

  const connection = createConnection()

  //PAGINATION FOR PATIENTS
  limit = limit ? limit : 5
  const whereClause = startKey ? " WHERE concat(p.lastName, p.ID) > '" + startKey + "'" : ""

  let sql = 'SELECT p.*, concat(p.lastName, p.ID) as startKey '
  sql += ' FROM patientWithConditions p '
  sql += ' INNER JOIN (SELECT DISTINCT ID '
  sql += ' FROM patientWithConditions p'
  sql += whereClause
  sql += ' LIMIT ' + limit + ') b '
  sql += ' ON p.ID = b.ID '
  sql += whereClause
  sql += ' ORDER BY startKey'

  console.log("pagination sql for patients: ", sql)


  //let sql = 'SELECT * FROM pharmaStudent.patientWithConditions;'

  connection.query(sql, function(err, data) {
  if (err) return cb(errorMessage)

  if (data.length === 0) return cb(noDataFound)

      cb(null, formatMultiplePatients(data))
})

}

/////////////////////
// PATIENT HELPER FUNCTIONS
/////////////////////
function formatSinglePatient(patientRows) {

  const mappedConditions = compose(
    map(patient => patient.condition),
    filter(patient => patient.condition)
  )(patientRows)

  return  compose(
    set(lensProp('birthdate'), moment(prop('birthdate', patientRows[0])).format("YYYY-MM-DD")),
    set(lensProp('conditions') , mappedConditions),
    omit(['ID', 'condition']),
    set(lensProp('_id'), toString(prop('ID', patientRows[0]))),
    set(lensProp('_rev'), ""),
    set(lensProp('type'), "patient")
  )(patientRows[0])
}


function formatMultiplePatients(patients) {

  const IDs = compose(
  uniq(),
  map(patient => patient.ID)
)(patients)

 return map(id => compose(
formatSinglePatient,
filter(patient => patient.ID === id)
)(patients))(IDs)

}


/////////////////////
// PHARMACIES
/////////////////////

function getPharmacy(pharmacyId, cb) {

  if(!pharmacyId) return cb({
    error: 'missing_id',
    reason: 'missing_id',
    name: 'missing_id',
    status: 400,
    message: 'unable to retrieve data due to missing id.'
  })

  const connection = createConnection()

  connection.query('SELECT * FROM pharmacy WHERE ID = ?', [pharmacyId], function (err, data) {
    if (err) return cb({
        error: 'unknown',
        reason: 'unknown',
        name: 'unknown',
        status: 500,
        message: err.message
    })
    if (data.length === 0) return cb({
        error: 'not_found',
        reason: 'missing',
        name: 'not_found',
        status: 404,
        message: 'missing'
    })

    const typeLens = lensProp('type');
    const revLens = lensProp('_rev');
    const idLens = lensProp('_id')

    set(revLens, 'pharmacy', data[0]);
    set(typeLens, 'pharmacy', data[0]);

    let idValue = prop('ID', data[0])
    idValue = toString(idValue)

    const theResult = compose(
      omit('ID'),
      set(idLens, idValue),
      set(revLens, ''),
      set(typeLens, 'pharmacy')
    )(data[0])

    cb(null, theResult)

  })
}


/////////////////////
// helper functions
/////////////////////

function createConnection() {
    return mysql.createConnection({
        host: "0.0.0.0",
        user: "root",
        password: "viewsonic",
        database: "pharmaStudent"
    });
}


function getDocByID(tablename, id, formatter, callback) {
    //  console.log("getDocByID", tablename, id)
    if (!id) return callback({
        error: 'missing_id',
        reason: 'missing_id',
        name: 'missing_id',
        status: 400,
        message: 'unable to retrieve data due to missing id.'
    })

    var connection = createConnection()

    connection.query('SELECT * FROM ' + connection.escapeId(tablename) + ' WHERE id = ?', [id], function(err, data) {
        if (err) return callback({
            error: 'unknown',
            reason: 'unknown',
            name: 'unknown',
            status: 500,
            message: err.message
        })
        if (data.length === 0) return callback({
            error: 'not_found',
            reason: 'missing',
            name: 'not_found',
            status: 404,
            message: 'missing'
        });

        if (data) {
            //console.log("query returned with data", formatter, formatter(data[0]))
            // grab the item sub [0] from the data.
            // take the data and run it through the formatter (convertPersonNoSQLFormat)
            // then take result of converting the person and parseToJSON
            return callback(null, formatter(data))
        }
    });
    connection.end(function(err) {
        if (err) return err;
    });
}

// function formatMed() {
//     return compose(
//         addMedType,
//         convertNoSQLFormat
//     )
// }

function formatMed(meds) {
  // db view returns repeated meds when multiple ingredients.
  // First, return a single med by grabbing the first med in the array.
  //  Then, format the med to make it look like a couchdb doc.

    return compose(
        addMedType,
        convertNoSQLFormat
    )(view(lensIndex(0), meds))


    //console.log("Meds from mysql",)










    //
    // const formattedMed = compose(
    //     addMedType,
    //     convertNoSQLFormat
    // )(view(lensIndex(0), meds))
    //
    // // create an array of ingredients from the db view rows
    // const ingredients = compose(
    //     filter(med => med !== null),
    //     map(med => path(['ingredient'], med))
    // )(meds)
    //
    // // set a new ingredient property with the array of ingredients
    // //  before returning the formatted med.
    // return set(lensPath(['ingredient']), ingredients, formattedMed );
}

const addMedType = med => set(lensPath(['type']), 'medication', med)

const convertNoSQLFormat = row => compose(
      omit(['ID']),
      set(lensPath(['_id']), path(['ID'], row))
    )(row)


////////////////////////////////
// ERROR RESPONSE HELPERS
////////////////////////////////

function notFound() {
  return {
      error: 'missing_id',
      reason: 'missing_id',
      name: 'missing_id',
      status: 400,
      message: 'Unable to retrieve data due to missing id.'
  }
}

function errorMessage() {
  return {
      error: 'unknown',
      reason: 'unknown',
      name: 'unknown',
      status: 500,
      message: err.message
  }
}

function noDataFound() {
  return {
      error: 'not_found',
      reason: 'missing',
      name: 'not_found',
      status: 404,
      message: 'missing'
  }
}
