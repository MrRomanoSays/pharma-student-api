const express = require('express')
const app = express()
const {
    getUniqueForms,
    listMedsByLabel,
    getUniqueIngredients,
    listMedsByIngredient,
    listMedsByForm,
    getMed,
    addPatient
} = require('./dal.js')
const {
    split
} = require('ramda')
const HTTPError = require('node-http-error')
const port = process.env.PORT || 8080
const bodyParser = require('body-parser')

app.use(bodyParser.json())




app.get('/medications', function(req, res, next) {
    if (req.query.filter && split(':', req.query.filter)[0] === 'ingredient') {
        const result = split(':', req.query.filter)
        listMedsByIngredient(result[1], function(err, meds) {
            if (err) return next(new HTTPError(err.status, err.message, err))
            res.status(200).send(meds)
        })
    } else if (req.query.filter && split(':', req.query.filter)[0] === 'form') {
        const result = split(':', req.query.filter)
        listMedsByForm(result[1], function(err, meds) {
            if (err) return next(new HTTPError(err.status, err.message, err))
            res.status(200).send(meds)
        })
    } else if (!req.query.filter) {
        listMedsByLabel(function(err, meds) {
            if (err) return next(new HTTPError(err.status, err.message, err))
            res.status(200).send(meds)
        })
    } else {
        res.status(200).send([])
    }
})

app.get('/medications/ingredients', function (req, res, next) {
  getUniqueIngredients(function (err, ingredients) {
    if (err) return next(new HTTPError(err.status, err.message, err))
    res.status(200).send(ingredients)
  })
})

app.get('/medications/forms', function (req, res, next) {
  getUniqueForms(function (err, forms) {
    if (err) return next(new HTTPError(err.status, err.message, err))
    res.status(200).send(forms)
  })
})

app.post('/patients', function (req, res, next) {
  console.log(req.body)
  addPatient(req.body, function (err, dalResponse) {
    if (err) return next(new HTTPError(err.status, err.message, err))
    res.send(dalResponse)
  })
})



///////////// error handler /////////////////////////////
app.use(function(err, req, res, next) {
    console.log(req.method, " ", req.path, "error:  ", err)
    res.status(err.status || 500)
    res.send(err)
})

app.listen(port, function() {
    console.log("I'm listening on port ", port)
})
