const mysql = require('mysql');

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
    lensPath
} = require('ramda')


var dal = {
    getMed: getMed
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

/////////////////////
// helper functions
/////////////////////

function createConnection() {
    return mysql.createConnection({
        host: "0.0.0.0",
        user: "root",
        password: "mypassword",
        database: "test-pharma-student"
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
