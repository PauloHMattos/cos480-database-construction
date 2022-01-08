#include "pch.h"
#include "FixedRecord.h"


Schema* FixedRecord::CreateSchema() {
    auto fixedSchema = new Schema();
    fixedSchema->AddColumn(NAMEOF(Gender).str(), ColumnType::CHAR);
    fixedSchema->AddColumn(NAMEOF(FirstName).str(), ColumnType::CHAR, 50);
    fixedSchema->AddColumn(NAMEOF(LastName).str(), ColumnType::CHAR, 50);
    fixedSchema->AddColumn(NAMEOF(StreetAddress).str(), ColumnType::CHAR, 100);
    fixedSchema->AddColumn(NAMEOF(City).str(), ColumnType::CHAR, 40);
    fixedSchema->AddColumn(NAMEOF(State).str(), ColumnType::CHAR, 3);
    fixedSchema->AddColumn(NAMEOF(ZipCode).str(), ColumnType::CHAR, 10);
    fixedSchema->AddColumn(NAMEOF(Email).str(), ColumnType::CHAR, 100);
    fixedSchema->AddColumn(NAMEOF(Username).str(), ColumnType::CHAR, 20);
    fixedSchema->AddColumn(NAMEOF(Password).str(), ColumnType::CHAR, 20);
    fixedSchema->AddColumn(NAMEOF(PhoneNumber).str(), ColumnType::CHAR, 18);
    fixedSchema->AddColumn(NAMEOF(Age).str(), ColumnType::INT32);
    fixedSchema->AddColumn(NAMEOF(Weigth).str(), ColumnType::FLOAT);
    fixedSchema->AddColumn(NAMEOF(Height).str(), ColumnType::INT32);
    return fixedSchema;
}