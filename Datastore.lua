local Players = game:GetService("Players")
local ReplicatedSotrage = game:GetService("ReplicatedStorage")
local RunService = game:GetService("RunService")
local DataStoreService = game:GetService("DataStoreService")

local SyncData = ReplicatedSotrage
					:WaitForChild("Remotes")
					:WaitForChild("SyncData")
local IsClient = RunService:IsClient()
local StoreStrings = {"Character", "Extra"}
local function SetTablePath(Table, Path, Value)
	local PathSize = #Path
	local Temp = Table
	for Index, Value in Path do
		if Index == PathSize then
			break
		end 
		Temp = Temp[Value]					
	end
	Temp[Path[PathSize]] = Value
end
--- /// Signal
local Connection = {}
Connection.__index = Connection

function Connection.new(Signal, Func, IsInstantaneous)
	return setmetatable({
		Func = Func,
		Signal = Signal,
		IsInstantaneous = IsInstantaneous,
	}, Connection)
end
function Connection:Disconnect()
	local Signal = self.Signal
	local Index = table.find(Signal.Connections, self)
	if Index then
		table.remove(Signal.Connections, Index)
	end
end
local Signal = {}
Signal.__index = Signal

function Signal.new()
	return setmetatable({
		Connections = {},
		HasFired = false,	
	}, Signal)
end
function Signal:Connect(IsInstantaneous, Func)
	local Handler = Connection.new(self, Func, IsInstantaneous)
	table.insert(self.Connections, Handler)
	return Handler
end
function Signal:DisconnectAll()
	table.clear(self.Connections)
end
function Signal:Fire(...)
	local Size = #self.Connections
	if Size == 0 then
		return
	end
	for Index = 1, Size do
		local Handler = self.Connections[Index]
		if self.IsInstantaneous then
			Handler.Func(...)
		else
			task.spawn(Handler.Func, ...)
		end
	end
	self.HasFired = true
end
function Signal:Wait()
	local WaitingCoroutine = coroutine.running()
	local _Connection = nil
	_Connection = self:Connect(true, function(...)
		_Connection:Disconnect()
		task.spawn(WaitingCoroutine, ...)
	end)
	return coroutine.yield()
end
--- /// 
local PlayersData = {}
local DataChangedSignals = {}
if IsClient then
	local function GetUnsplitPath(Array)
		local Buffer = ""
		for _, Value in Array do
			Buffer..=(Value.."/")
		end
		Buffer = string.sub(Buffer, 0, -2)
		return Buffer
	end
	local DataChangedSignal = Signal.new()
	SyncData.OnClientEvent:Connect(function(Path, Data, StoreString)
		SetTablePath(PlayersData[StoreString], Path, Data)
		DataChangedSignal:Fire(GetUnsplitPath(Path))
	end)
	for _, StoreString in StoreStrings do
		PlayersData[StoreString] = {}
	end
	return {
		PlayersData = PlayersData,
		DataChangedSignal = DataChangedSignal,
	}
end
local MARGIN_OF_SAFETY = 1.25
local MAX_GET_ASYNC_ATTEMPTS = 6
local RETRY_GET_ASYNC_YIELD = 1
local Success, Value = 1, 2
local AutoSaveTime
local function SetUnion(A, B)
	local Union = {}
	for _, Value in A do
		table.insert(Union, Value)
	end
	for _, Value in B do
		table.insert(Union, Value)
	end
	return Union
end
local function GetTablePath(Table, Path)
	local Temp = Table
	for Index, Value in Path do
		Temp = Temp[Value]					
	end
	return Temp
end
local function SizeOfTable(Table)
	local Count = 0
	for _, _ in Table do
		Count += 1
	end
	return Count
end
local function WaitForRequestBudget(Request)
	while DataStoreService:GetRequestBudgetForRequestType(Request) < MAX_GET_ASYNC_ATTEMPTS + 1 do
		task.wait()
	end
end
local ReplicatorAnnotation = {}
ReplicatorAnnotation.__tostring = function()
	return "Replicator"
end
local function CreateReplicator(Object, StoreString)
	local self = nil
	self = setmetatable({
		Object = Object,
		AttachPath = function(Path)
			self.Path = Path
		end,
		YieldUntilPath = function(Time)
			local Accumulator = 0
			while not self.Path do
				Accumulator += task.wait()
				if Time and Accumulator >= Time then
					break
				end
			end 
		end,
		Change = function(Value, ...)
			if not self.Path then return end
			self.Object = Value
			local Whitelist = {...}
			if next(Whitelist) then
				for _, Player in Whitelist do
					SyncData:FireClient(Player, self.Path, Value, StoreString)
				end
			else
				SyncData:FireAllClients(Value, self.Path, Value, StoreString)
			end
		end,
	}, ReplicatorAnnotation)
	return self
end
local function CreateReplicatorWrapper(StringStore)
	return function(Object)
		return CreateReplicator(Object, StringStore)
	end
end
local ReplicatorWrappers = {
	Character = CreateReplicatorWrapper("Character"),
	Extra = CreateReplicatorWrapper("Extra"),
}
local function DeepCopy(Table, StoreString)
	local ReplicatorWrapper = ReplicatorWrappers[StoreString] or CreateReplicator
	local Copy = {}
	for Key, Value in Table do
		if type(Value) == "table" then
			if not (tostring(Value) == "Replicator") then
				Value = DeepCopy(Value)
			else
				local CopiedObject = nil
				if type(Value.Object) == "table" then
					print(Value.Object)
					CopiedObject = DeepCopy(Value.Object, StoreString)
				else
					CopiedObject = Value.Object
				end
				local ReplicatorCopy = ReplicatorWrapper(CopiedObject)
				ReplicatorCopy.Path = Value.Path
			end
		end
		Copy[Key] = Value
	end
	return Copy
end
local function AttachPathsToReplicators(Table, Path)
	for Key, Value in Table do
		if type(Value) == "table" then
			if tostring(Value) == "Replicator" then
				local UnionPath = SetUnion(Path or {}, {Key})
				Value.AttachPath(UnionPath)
				if type(Value.Object) == "table" then
					AttachPathsToReplicators(Value.Object, SetUnion(UnionPath, {"Object"}))
				end
			else
				AttachPathsToReplicators(Value, SetUnion(Path or {}, {Key}))
			end
		end
	end
end
local CreateReplicatorCharacter = ReplicatorWrappers.Character
local CreateReplicatorExtra = ReplicatorWrappers.Extra
local STORES = {
	Character = {
		Store = DataStoreService:GetDataStore("Character"),
		Default = {
			Antares = CreateReplicatorCharacter(5)
		}
	},
	Extra = {
		Store = DataStoreService:GetDataStore("Extra"),
		Default = {}
	},
}
local function MergeReplicatorTemplate(Table, StoreString, Descendant)
	local ReplicatorWrapper = ReplicatorWrappers[StoreString]
	local ShadowDescendant = Descendant or STORES[StoreString].Default
	for Key, Value in ShadowDescendant do
		if type(Value) == "table" then
			if tostring(Value) == "Replicator" then
				local ReplicatorCopy = unpack(DeepCopy({Value}, StoreString))
				SetTablePath(Table, Value.Path, ReplicatorCopy)
				if type(Value.Object) == "table" then
					MergeReplicatorTemplate(Table, StoreString, Value.Object)
				end
			else
				MergeReplicatorTemplate(Table, StoreString, Value)
			end
		end
	end
	if not Descendant then
		return Table
	end
end
local UniqueTypeKey = "Senpai"
local DatastoreKey = "Player_"
local function Retry(YieldTime, Attempts, Func, Attempt)
	Attempt = Attempt or 1
	local Args = {pcall(Func)}
	if Attempt == Attempts then
		return unpack(Args)
	end
	if Args[Success] then
		return unpack(Args)
	else
		if YieldTime then
			task.wait(YieldTime)
		end
		return Retry(YieldTime, Attempts, Func, Attempt + 1)
	end
end
local function WrapSerializedValue(Serialized, Type)
	return {
		[UniqueTypeKey] = Type,
		Serialized = Serialized
	}
end
local function Serialize(Table)
	local Modified = {}
	for Key, Value in Table do
		local Type = typeof(Value)
		if Type == "table" then
			if tostring(Value) == "Replicator" then
				if type(Value.Object) == "table" then
					Value = Serialize(Value.Object)
				else
					Value = Value.Object
				end
			else
				Value = Serialize(Value)
			end
		elseif Type == "Vector2" then
			Value = WrapSerializedValue({Value.X, Value.Y}, Type)
		elseif Type == "UDim2" then
			Value = WrapSerializedValue({Value.X.Scale, Value.X.Offset, Value.Y.Scale, Value.Y.Offset}, Type)
		elseif Type == "Vector3" then
			Value = WrapSerializedValue({Value.X, Value.Y, Value.Z}, Type)
		elseif Type == "Color3" then
			Value = WrapSerializedValue({Value.R, Value.G, Value.B}, Type)
		elseif Type == "CFrame" then
			Value = WrapSerializedValue({Value:GetComponents()}, Type)
		end
		Modified[Key] = Value
	end
	return Modified
end
local function Deserialize(Table)
	local Unmodified = {}
	for Key, Value in Table do
		if type(Value) == "table" then
			local RawTypeKey = Value[UniqueTypeKey]
			if RawTypeKey then
				if RawTypeKey == "Vector2" then
					Value = Vector2.new(unpack(Value.Serialized))
				elseif RawTypeKey == "UDim2" then
					Value = UDim2.new(unpack(Value.Serialized))
				elseif RawTypeKey == "Vector3" then
					Value = Vector3.new(unpack(Value.Serialized))
				elseif RawTypeKey == "Color3" then
					Value = Color3.new(unpack(Value.Serialized))
				elseif RawTypeKey == "CFrame" then
					Value = CFrame.new(unpack(Value.Serialized))
				end
			else
				Value = Deserialize(Table)
			end
		end
		Unmodified[Key] = Value
	end
	return Unmodified
end
for _, Store in STORES do
	AttachPathsToReplicators(Store.Default)
end
local Module = {}
Module.PlayersData = PlayersData
function Module.GetData(Player, StoreString, JustJoined)
	WaitForRequestBudget(Enum.DataStoreRequestType.GetAsync)
	local Store = STORES[StoreString].Store
	local Success, Data = Retry(nil, MAX_GET_ASYNC_ATTEMPTS, function()
		return Store:GetAsync(DatastoreKey..Player.UserId)
	end)
	if JustJoined and Data and Data.SessionBool then
		local _Success, _Data = Retry(
			RETRY_GET_ASYNC_YIELD, 
			MAX_GET_ASYNC_ATTEMPTS, 
			function()
				local NewData = Store:GetAsync(DatastoreKey..Player.UserId)
				return not Data.SessionBool
			end
		)
		if not _Success then
			Data = nil
			warn("Error while attempting to session lock")
		end
	end
	if Success then
		print("Successfully retrieved data")
	elseif type(Data) == "string" then
		warn("Shit")
		return
	end
	if Data then
		Data = MergeReplicatorTemplate(Deserialize(Data), StoreString)
	else
		Data = DeepCopy(STORES[StoreString].Default, StoreString)
	end
	Data.DataId = Data.DataId or 1
	Data.SessionBool = true
	if JustJoined then
		if not PlayersData[Player.Name] then
			PlayersData[Player.Name] = {}
		end
		PlayersData[Player.Name][StoreString] = Data
	end
	return Success, Data
end
function Module.UpdateData(Player, StoreString, IsLeaving)
	WaitForRequestBudget(Enum.DataStoreRequestType.UpdateAsync) -- Backup that will most likely never yield
	local Store = STORES[StoreString].Store
	Store:UpdateAsync(DatastoreKey..Player.UserId, function(OldData)
		local PreviousData = OldData or {DataId = 1}
		local CurrentData = PlayersData[Player.Name][StoreString]
		if CurrentData.DataId == PreviousData.DataId then
			if IsLeaving then
				CurrentData.DataId += 1
			end
			return Serialize(CurrentData)
		end	
		return nil
	end)
end
function Module.RawGetData(PlayerName, StoreString, Path)
	local Split = string.split(Path, "/")
	return GetTablePath(PlayersData[PlayerName][StoreString], Split)
end
function Module.RawUpdateData(PlayerName, StoreString, Path, Value)
	local Split = string.split(Path, "/")
	local Data = GetTablePath(PlayersData[PlayerName][StoreString], Split)
	if type(Data) == "table" then
		if tostring(Data) == "Replicator" then
			Data.Change(Value)
		end
	else
		SetTablePath(PlayerName, StoreString, Split)
	end
	DataChangedSignals[PlayerName][StoreString]:Fire(Path)
end
local StoresSize = SizeOfTable(STORES)
local Budget = nil
local Threads = {}
local Clock = os.clock()
local CanStart = false
RunService.Heartbeat:Connect(function()
	if not CanStart then 
		return 
	end
	local CurrentBudget = DataStoreService:GetRequestBudgetForRequestType(Enum.DataStoreRequestType.UpdateAsync)
	local PlayerCount = #Players:GetPlayers()
	if CurrentBudget ~= Budget then
		Budget = CurrentBudget
		AutoSaveTime = math.ceil(((PlayerCount * StoresSize * 6) / (PlayerCount + 6)) * MARGIN_OF_SAFETY)
	end
	if not AutoSaveTime then 
		return 
	end
	if os.clock() - Clock > AutoSaveTime then
		Clock = os.clock()
		for _, PlayerObject in Threads do
			for _, CoroutineWrapper in PlayerObject do
				coroutine.resume(CoroutineWrapper.Coroutine)
			end 
		end
	end
end)
local function OnPlayerAdded(Player)
	Player.CharacterAdded:Wait()
	local function UpdateReplicators(Table)
		for _, Value in Table do
			print(Value)
			if tostring(Value) == "Replicator" then
				Value.Change(Value.Object, Player)
				if type(Value.Object) == "table" then
					UpdateReplicators(Value.Object)
				end
			elseif type(Value) == "table" then
				UpdateReplicators(Value)
			end
		end
	end
	Threads[Player] = {}
	for StoreString, _ in STORES do
		Threads[Player][StoreString] = {
			IsResumed = false,
			IsAlive = true,
		}
		local CoroutineWrapper = Threads[Player][StoreString]
		Threads[Player][StoreString].Coroutine = coroutine.create(function()
			while true do
				if not CoroutineWrapper.IsAlive then
					return
				end
				CoroutineWrapper.IsResumed = true
				Module.UpdateData(Player, StoreString)
				CoroutineWrapper.IsResumed = false
				coroutine.yield()
			end
		end)
	end
	DataChangedSignals[Player.Name] = {}
	for StoreString, _ in STORES do
		DataChangedSignals[Player.Name][StoreString] = Signal.new()
		Module.GetData(Player, StoreString, true)
		UpdateReplicators(PlayersData[Player.Name][StoreString])	
	end
end
Players.PlayerAdded:Connect(OnPlayerAdded)
Players.PlayerRemoving:Connect(function(Player)
	if not Player.Character then return end
	local PlayerObject = Threads[Player]
	for StoreString, _ in STORES do
		local CoroutineWrapper = PlayerObject[StoreString]
		CoroutineWrapper.IsAlive = false
		while CoroutineWrapper.IsResumed do
			task.wait()
		end
		coroutine.resume(CoroutineWrapper.Coroutine)
	end
	task.wait()
	Threads[Player] = nil
	for StoreString, _ in STORES do
		DataChangedSignals[Player.Name][StoreString]:DisconnectAll()
		DataChangedSignals[Player.Name][StoreString] = nil
		PlayersData[Player.Name][StoreString].SessionBool = false
		Module.UpdateData(Player, StoreString, true)	
	end
	PlayersData[Player.Name] = nil
	DataChangedSignals[Player.Name] = nil
end)
game:BindToClose(function()
	wait(1)
end)
for _, Player in Players:GetPlayers() do
	OnPlayerAdded(Player)
end
task.wait(2)
CanStart = true
return {
	Service = Module,
	PlayersData = PlayersData,
	DataChangedSignals = DataChangedSignals,
}
