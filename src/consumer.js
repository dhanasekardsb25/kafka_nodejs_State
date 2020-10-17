const { Kafka } = require('kafkajs')
const config = require('./config')

const kafka = new Kafka({
  clientId: config.kafka.CLIENTID,
  brokers: config.kafka.BROKERS
})

const topic = config.kafka.TOPIC
const consumer = kafka.consumer({
  groupId: config.kafka.GROUPID
})

const run = async () => {
  await consumer.connect()
  await consumer.subscribe({ topic, fromBeginning: true })
  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const jsonObj = JSON.parse(message.value.toString())
        let customerInfo = filterCustomerInfo(jsonObj)
        if (customerInfo) {
          console.log(
            '******* Alert!!!!! customerInfo *********',
            customerInfo
          )
        }
      } catch (error) {
        console.log('err=', error)
      }
    }
  })
}

function filterCustomerInfo(jsonObj) {
  let returnVal = null

  console.log(`eventId ${jsonObj.eventId} received!`)
	/*
  if (jsonObj.message == "Apartment Service Request") {
    returnVal = jsonObj
	serviceState()
  } */
  
  switch(jsonObj.message) {
	  case "Apartment Service Request" :
		returnVal = jsonObj
		serviceState()
	  break;
	  case "New apartment Reservation" :
		returnVal = jsonObj
		onboardState()
	  break;
	  case "Apartment payment processing" :
		returnVal = jsonObj
		reviewState()
	  break;
	  case "Renew lease agreement" :
		returnVal = jsonObj
		renewalState()
	  break;
	  default:
	  
	}
  
  return returnVal
}

function serviceState() {
	console.log("SERVICE STATE FUNCTION")
}

function onboardState() {
	console.log("On Board STATE FUNCTION")
}

function reviewState() {
	console.log("Review STATE FUNCTION")
}

function renewalState() {
	console.log("Renewal STATE FUNCTION")
}

run().catch(e => console.error(`[example/consumer] ${e.message}`, e))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
  process.on(type, async e => {
    try {
      console.log(`process.on ${type}`)
      console.error(e)
      await consumer.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.map(type => {
  process.once(type, async () => {
    try {
      await consumer.disconnect()
    } finally {
      process.kill(process.pid, type)
    }
  })
})

module.exports = {
  filterCustomerInfo
}
