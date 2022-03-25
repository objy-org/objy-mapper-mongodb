# OBJY storage mapper for mongodb

## Installing

```
npm install objy-mapper-mongodb
```


## Example

Let's create an Object Family that uses the mapper:

```
const MongoMapper = require('objy-mapper-monodb');

// Define an object family
OBJY.define({
   name : "Object",
   pluralName: "Objects",
   storage: new MongoMapper().connect('mongodb://localhost'),
})

// Use the object family's constructor
OBJY.Object({name: "Hello World"}).add(function(data)
{
   console.log(data);
})
```

## License

This project itself is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details. 
