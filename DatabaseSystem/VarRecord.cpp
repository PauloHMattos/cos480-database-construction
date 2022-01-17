#include "pch.h"
#include "VarRecord.h"


Schema* VarRecord::CreateSchema() {
    auto varSchema = new Schema();
    varSchema->AddColumn(NAMEOF(Gender).str(), ColumnType::CHAR);
    varSchema->AddColumn(NAMEOF(FirstName).str(), ColumnType::VARCHAR, 50);
    varSchema->AddColumn(NAMEOF(LastName).str(), ColumnType::VARCHAR, 50);
    varSchema->AddColumn(NAMEOF(StreetAddress).str(), ColumnType::VARCHAR, 100);
    varSchema->AddColumn(NAMEOF(City).str(), ColumnType::VARCHAR, 40);
    varSchema->AddColumn(NAMEOF(State).str(), ColumnType::VARCHAR, 3);
    varSchema->AddColumn(NAMEOF(ZipCode).str(), ColumnType::VARCHAR, 10);
    varSchema->AddColumn(NAMEOF(Email).str(), ColumnType::VARCHAR, 100);
    varSchema->AddColumn(NAMEOF(Username).str(), ColumnType::VARCHAR, 20);
    varSchema->AddColumn(NAMEOF(Password).str(), ColumnType::VARCHAR, 20);
    varSchema->AddColumn(NAMEOF(PhoneNumber).str(), ColumnType::VARCHAR, 18);
    varSchema->AddColumn(NAMEOF(Age).str(), ColumnType::INT32);
    varSchema->AddColumn(NAMEOF(Weigth).str(), ColumnType::FLOAT);
    varSchema->AddColumn(NAMEOF(Height).str(), ColumnType::INT32);
    return varSchema;
}