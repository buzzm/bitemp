/**
            
systemDate  D1
bizDate     D1
value       v2

systemDate  D2
bizDate     D2
value       v2

 **/

var dbname = "testX";
var collname = "bitemp"; // this will be DROPPED and repopulated

var db = db.getSiblingDB(dbname);

db[collname].drop();

var infinity_date = new ISODate("99991231");
var d1 = new ISODate("20190501");
var d2 = new ISODate("20190601");
var d3 = new ISODate("20190701");
var d4 = new ISODate("20190801");

function changeHistory(bizkey, vers, newdate, data) {
    var xx = db[collname].findOne({"d.bizkey": bizkey, "d.v":vers});

    db[collname].update({"_id": xx['_id']}, {$set: {"d.hdr.eed":newdate}});

    db[collname].insert({d: { hdr: { createDate: newdate, esd: newdate, eed: infinity_date }, bizkey: bizkey, cv: xx['d']['cv'], v: xx['d']['v'], data: data }});
}

function logicalUpdate(bizkey, newdate, data) {
    // logical update, two things happen:
    // The cv on the old record is set to 2.   It will now have cv:2 and v:1.
    // This means this record was superceded by v:2
    // The cv on the new record is set to 0 and the v is 2
    var xx = db[collname].findOne({"d.bizkey": bizkey, "d.cv":0});
    var newv = xx['d']['v'] + 1;
    db[collname].update({"_id": xx['_id']}, {$set: {"d.cv":newv}});
    db[collname].insert({d: { hdr: { createDate: newdate, esd: newdate, eed: infinity_date }, bizkey: bizkey, cv: 0, v: newv, data: data }});
}


// Insert item with bizkey "AA", version 1.  The cv:0 is an indexing 
// optimization trick.   cv:0 is always the most current version.
// esd means effective start date
// eed means effective end date
db[collname].insert({d: { hdr: { createDate: d1, esd: d1, eed: infinity_date }, bizkey: "AA", cv: 0, v: 1, data: "buzz" }});

// Insert another item item with bizkey BB, version 1:
db[collname].insert({d: { hdr: { createDate: d1, esd: d1, eed: infinity_date }, bizkey: "BB", cv: 0, v: 1, data: "dave"  }});


// Update current record AA.  The payload is not important here.
logicalUpdate("AA", d2, "bob");

// Once more for fun:
logicalUpdate("AA", d3, "jane");

// What is the most current "AA" as of d3?
var xx = db[collname].findOne({"d.bizkey": "AA", "d.cv":0});
printjson(xx);

// What is is version 2 of "AA" as of d3?
var xx = db[collname].findOne({"d.bizkey": "AA", "d.v":2});
printjson(xx);

// OK.  Now, on d4, we realize that the "bob" data in v:2 is broken and we
// need to change it.
changeHistory("AA", 2, d4, "BOBBY");

// Now lets bring bitemp fields into play:
// What is is version 2 of "AA" as of d3? 
var xx = db[collname].findOne({"d.bizkey": "AA"
			       ,"d.v":2
			       ,"d.hdr.esd":{"$lte":d3}
			       ,"d.hdr.eed":{"$gt":d3}
    });
printjson(xx);

// What is is version 2 of "AA" as of d4? 
var xx = db[collname].findOne({"d.bizkey": "AA"
			       ,"d.v":2
			       ,"d.hdr.esd":{"$lte":d4}
			       ,"d.hdr.eed":{"$gt":d4}
    });
printjson(xx);

// For a little more, get the version history as of d3:
db[collname].find({"d.bizkey": "AA"
			       ,"d.hdr.esd":{"$lte":d3}
			       ,"d.hdr.eed":{"$gt":d3}
    }).forEach(function(doc) {
	    print("v: " + doc['d']['v'] + "; data: " + doc['d']['data']);
	});


// And now as of d4:
db[collname].find({"d.bizkey": "AA"
			       ,"d.hdr.esd":{"$lte":d4}
			       ,"d.hdr.eed":{"$gt":d4}
    }).forEach(function(doc) {
	    print("v: " + doc['d']['v'] + "; data: " + doc['d']['data']);
	});


/*
Jury is out on whether we can cheat and use createdate to represent
esd (effective start date).
Also:  Note that this bitemp framework imposes no conditions or actions 
on items that might change as a result of changing history.  It is up the 
logic to determine if it is necessary to do something to all the versions
AFTER the changed one,
*/