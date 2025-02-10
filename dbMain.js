const fs = require("fs");
const axios = require("axios");
const { parse } = require("json2csv");
const csv = require("csv-parser");
const { MongoClient } = require("mongodb");

const targetDate = "27-Dec-2024";
const uri =
  "mongodb+srv://balu:busdata123%40gmail.com@revenuedata.upijd.mongodb.net/"; // Replace with your MongoDB connection string

const CITIES = {
  Mumbai: 462,
  Hyderabad: 124,
  Bangalore: 122,
  Chennai: 123,
  Vizag: 248,
  Vijayawada: 134,
  Guntur: 137,
  Tirupati: 71756,
  Madurai: 126,
  Coimbatore: 141,
  Trichy: 71929,
  Pondicherry: 233,
  Pune: 130,
  Indore: 313,
  Nagpur: 624,
  Goa: 210,
  Warangal: 95479,
  Nellore: 131,
  Kochi: 216,
  Dharwad: 167,
  Mysore: 129,
  Mangaluru: 95222,
  Thiruvananthapuram: 71425,
};

async function connectToMongoDB() {
  const client = new MongoClient(uri);
  await client.connect();
  const database = client.db("redBusLive");
  const collection = database.collection("seatDataCollection");
  return { client, collection };
}

async function uploadSeatDataToMongoDB(collection, travelsName, serviceId, targetDate, seatData, presentDateAndTime) {
    const update = {
      $set: { lastUpdated: presentDateAndTime },
      $setOnInsert: { travelsName, serviceId, targetDate },
    };
  
    for (const [seat, data] of Object.entries(seatData)) {
      update.$push = update.$push || {};
      update.$push[`seatData.${seat}`] = { $each: data };
    }
  
    await collection.updateOne(
      { travelsName, serviceId, targetDate },
      update,
      { upsert: true }
    );
  
    console.log(`Updated seat data for ${travelsName}_${serviceId} on ${targetDate}`);
  }

function toIST(date) {
  const offset = 5.5 * 60 * 60 * 1000; // IST offset in milliseconds
  const istDate = new Date(date.getTime() + offset);
  return istDate.toISOString().slice(0, 16).replace("T", " ");
}

async function fetchRedbusSeatLayout(
  fromCityId,
  toCityId,
  operatorId,
  routeId,
  targetDate
) {
  var url = `https://www.redbus.in/search/seatlayout/${routeId}/${targetDate}/${operatorId}?isRedDealApplicable=false&tok=22135751698&srcCountry=IND&fromCity=${fromCityId}&toCity=${toCityId}&InvPos=1`;

  var options = {
    method: "GET",
    headers: {
      accept: "application/json, text/plain, */*",
      "accept-language": "en-US,en;q=0.9",
      cookie:
        'mriClientId=WD46d71904-1cde-440f-a806-81bf1b5348dc; _gcl_au=1.1.358583087.1732563226; _ga=GA1.1.1862293725.1732563227; rb_fpData=%7B%22browserName%22%3A%22Chrome%22%2C%22browserVersion%22%3A%22131.0.0.0%22%2C%22os%22%3A%22Windows%22%2C%22osVersion%22%3A%2210%22%2C%22screenSize%22%3A%221536%2C816%22%2C%22screenDPI%22%3A1.25%2C%22screenResolution%22%3A%221920x1080%22%2C%22screenColorDepth%22%3A24%2C%22aspectRatio%22%3A%2216%3A9%22%2C%22systemLanguage%22%3A%22en-US%22%2C%22connection%22%3A%224g%22%2C%22userAgent%22%3A%22mozilla/5.0%20%28windows%20nt%2010.0%3B%20win64%3B%20x64%29%20applewebkit/537.36%20%28khtml%2C%20like%20gecko%29%20chrome/131.0.0.0%20safari/537.36%7CWin32%7Cen-US%22%2C%22timeZone%22%3A5.5%7D; srcCountry=IND; destCountry=null; srpUserTypeVal=GUEST; singleSeatCoachMark=2; reddealBoostAB=V2; rtcInline=V2; dynamic_custinfo=V1; country=IND; currency=INR; defaultlanguage=en; language=en; selectedCurrency=INR; abExps=["reddealBoostAB","rtcInline","dynamic_custinfo"]; abExpsVariants=reddealBoostAB:V2?rtcInline:V2?dynamic_custinfo:V1; deviceSessionId=0d395e67-39e4-41a7-bc8f-bb37496418f0; reqOrigin=IND; lzFlag=0; bCore=0; defaultCountry=IND; CountryName=INDIA; _abck=FDB2C363EB118B39755E8CB83F8D5518~-1~YAAQJjkgF/FJ2aqTAQAA1TNo3w1o+eX9BpFTBEoKLMZ/c9t2plrolgzYQmPRmeWu/a0k9FQs0P3G3xscGmIVlpEkjTYLXFsMaQWlb0XdK93OygukHzrfYDyP3Z1Td6voDr4N3xibypE83t9SzLawCcHWmvnIjVNMCUY2ufjVh5xAYqhC0m34zR5suRoMmC2YuAyE/vHtWSP8uYuWmdtK3xAF7mo3byIhiZnChSNTrFfudHLzIoyQLrufATn2qvASSjdYndSjAXeLUvyZIq/68quiZ4C+PMemBYvGRlmrm1ASam4qoAL+DuBNlWNsgmOsQZfTxaNcYZw52rHjrsL9+te3at+5h4WdQkU9Xu3TeBVdinUxu81iVj4tLXLKy+WzwzXKgDwHgAxpNpC2oH35Nv9by+xV0sWO+FWJ5pjb0YpC33kib2zCMfRIFsp+1K7ZRESe4D1R0kSgImDM8U3wtAA=~-1~-1~-1; mriSessionId=WD66bb9769-1690-47eb-a43f-ea291eb02db5; mriClientIdSetDate=12%2F19%2F24%202%3A52%3A49%20PM; ak_bmsc=269C17AEFFCA9CEA8278AA97D4BE4D51~000000000000000000000000000000~YAAQJjkgFyVK2aqTAQAArzho3xo0+efMuaZGy6WmirYy9FOCBi+y/ZfIuxdU3wFVgq+lfp0blfPZcuOH2mYDOe6wBkJdFkvXhCcojS2xShKHgV2BlaCFjguCFsIhZXm2RRiY2f7IkyXf4u5GuDiOnqdJnd95AgyC3zFtJkU8ELXqIg7eo1fLikN81Hrc+L0v50bKFExqFTNGhyk9HgeSuNTCgq3qUyI+U83/nnBo7xm2B3UAEOaJXC3LLs0QHFDou4WTjx5jQ2dWmvJ2PVZnXQLyu25WOxdcT3GvR5JEFCgobs78C4lyiRKWALYvh7/yRzKyilriS+6ZScNPJGC1fk8QQwy5PS6VHfe39rGIh5FYK7zbLeSKUe4Q/HQqgHahGfqNbRMgObqvX1u5/LL1NcnXost9u8auy4mjAteEr+eIO0kUZYs4c8xllpWrNxsfCFywpQ==; bm_sz=38F0FDAAA8DB7CA463EA0148E02F4584~YAAQJjkgF5xK2aqTAQAALkFo3xpl/4l/HTuINpAAXPmrQdym+M/zsgO+aR2jPOWtLYd4lOuI34ExkzpWxwahGVB8yFYRB1rm4pp7i/JI9jNPmj2u72BrvmWeactS4wY/P6FYI9OI42kA0ICPS481PRKs6WT1tUhG9E+6jJNBkEnq5dE5eUgTfB4A4YFrxXBhRHL11BFLC37puvHZlJXpZtl5XUJAeGyWULc6jx6Rw5j4GfJDyVWlxMW6BONCcUR115+o3afbbaGFn3PSWDfbe5FPuYhF13TFWP40ImGfZg/GfK2Fn4GetUOheGox58vvJKcZpQSFjfbtMyATuN3nN7vRhCfiEYPLt0CYsvNRk0Yi1M7X2StmlNamir2cYVUkjlUtkN3QdTP8kCxWOTu4C50D~4403509~3420984; searchId=b8e35f38e3e4494eaf4747d9470f2f0a; isBrowserFP=true; bm_sv=8FD11C84AD19893663626D398827A9CC~YAAQJjkgF/1L2aqTAQAAvFho3xqlElhU7IcSMT7fIG3aQEojcH45XnPHhhJQXjK58QoXmD74T9DRXK0cq6Xi3EgnFfZfLx6D2D4buOC0Vl1Iwut1dibQQo1qviXkLz9MDykRVFfAIaSE69qf9hMtXRtPVD7bVAl51BAvjbzpuRgwJG3S9HfhKkmRRHyCA9s6vKDiFYjR8iOZUWFc/RTXunlULcwUcjtDnu9oj69hjZ9Jdz/y3Ep6iFjcW37eC5k=~1; resumeBook=true; _VTok=22135751698; _ga_1SE754V89Y=GS1.1.1734619970.9.1.1734619993.37.0.1959842207',
      priority: "u=1, i",
      referer: `https://www.redbus.in/bus-tickets?fromCityId=${fromCityId}&toCityId=${toCityId}&onward=${operatorId}&opId=0&busType=Any`,
      "sec-ch-ua":
        '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
      "sec-ch-ua-mobile": "?0",
      "sec-ch-ua-platform": '"Windows"',
      "sec-fetch-dest": "empty",
      "sec-fetch-mode": "cors",
      "sec-fetch-site": "same-origin",
      "user-agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    },
  };

  try {
    const response = await axios(url, options);
    console.log(`Fetched seat layout for routeId=${routeId}`);
    return response.data;
  } catch (error) {
    console.error("Error fetching data:", error);
    return "Error in fetching data";
  }
}

function responseJsonToMap(data) {
  const seatList = data.seatlist || [];
  const combinedData = seatList.reduce((acc, seat) => {
    const seatId = seat.Id;
    const faresAmount =
      seat.DP && seat.DP !== -1
        ? seat.DP
        : seat.OP || seat.fares?.amount || null;
    const isAvailable = seat.IsAvailable;
    acc[seatId] = { Fares_Amount: faresAmount, Is_Available: isAvailable };
    return acc;
  }, {});
  console.log("Mapped seat data:", combinedData);
  return combinedData;
}

function findFirstEmptyColumn(sheet, row) {
  console.log(`Finding first empty column in row ${row}`);
  row = row + 1;
  let col = 1; // Start from the first column
  let value = sheet[row - 1][col - 1];

  // Loop through the columns to find the first empty column
  while (value !== "" && value !== null) {
    col++;
    value = sheet[row - 1][col - 1];
  }

  return col; // Return the first empty column (1-based index)
}

async function mainHtoB() {
    console.log("Starting mainHtoB function");
    const inputFilePath = "intrCityBusesTrail.csv";
    const rows = [];
  
    fs.createReadStream(inputFilePath)
      .pipe(csv())
      .on("data", (row) => {
        rows.push(row);
      })
      .on("end", async () => {
        console.log("Finished reading CSV file");
  
        const { client, collection } = await connectToMongoDB();
  
        try {
          for (const map of rows) {
            console.log(`Processing row: ${JSON.stringify(map)}`);
            const fromCityId = CITIES[map["fromCity"]];
            const toCityId = CITIES[map["toCity"]];
            const operatorId = map["Operator ID"];
            const routeId = map["Route ID"];
            const travelsName = map["Travels Name"];
            const serviceId = map["Service ID"];
  
            console.log(`Fetching seat layout for fromCityId=${fromCityId}, toCityId=${toCityId}, operatorId=${operatorId}, routeId=${routeId}, targetDate=${targetDate}`);
            const responseJson = await fetchRedbusSeatLayout(
              fromCityId,
              toCityId,
              operatorId,
              routeId,
              targetDate
            );
            if (responseJson === "Error in fetching data") {
              console.error("Error in fetching data for:", map);
              continue;
            }
  
            console.log("Mapping seat data from response");
            const seatMappings = responseJsonToMap(responseJson);
            const allSeatsList = Object.keys(seatMappings).sort();
  
            const now = new Date();
            const presentDateAndTime = toIST(now);
            console.log(`Current IST time: ${presentDateAndTime}`);
  
            const seatData = {};
            allSeatsList.forEach(seat => {
              const fare = seatMappings[seat].Is_Available ? seatMappings[seat].Fares_Amount : 0;
              if (!seatData[seat]) {
                seatData[seat] = [];
              }
              seatData[seat].push({ fare, timestamp: presentDateAndTime });
            });
  
            console.log(`Uploading seat data for ${travelsName}_${serviceId} on ${targetDate}`);
            await uploadSeatDataToMongoDB(collection, travelsName, serviceId, targetDate, seatData, presentDateAndTime);
          }
        } finally {
          await client.close();
          console.log("Closed MongoDB connection");
        }
  
        console.log("Database update complete.");
      });
  }
  
  mainHtoB();
