#pragma once

class Serializable
{
public:
	virtual void Serialize(iostream& dst) = 0;
	virtual void Deserialize(iostream& src) = 0;
};

